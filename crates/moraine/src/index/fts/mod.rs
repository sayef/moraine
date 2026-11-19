//! The `fts` kind: a tantivy index per data file (spike item 9;
//! D40).
//!
//! **Build.** One tantivy document per row, in row order, including rows whose text is null or
//! empty (case F4), with one indexed, unstored text field per column and a `u64` fast field
//! `_row`. The writer runs on one thread with no merge policy; every part the driver seals is a
//! committed segment, and the finished build merges them, in row order, into one segment, so a
//! document id is the row offset. The segment's files become the bundle's sections;
//! equal rows give byte-identical bundles.
//!
//! **Open.** The sections are served in place by a read-only directory over the bundle bytes,
//! mapped from the disk cache or held in memory.
//!
//! **Search.** Not through the generic per-file route: BM25 needs table-wide statistics, so
//! the search gathers every file's searcher first (sidecar, or an index built in memory from
//! the rows), sums their statistics, and scores each file with the sums. Scores are then those
//! of one index over the whole table (T9, case F5).

pub(crate) mod dir;
pub(crate) mod search;
mod tokenizer;

use std::any::Any;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Schema as ArrowSchema};
use roaring::RoaringBitmap;
use serde_json::{Value, json};
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::directory::MmapDirectory;
use tantivy::index::SegmentComponent;
use tantivy::indexer::NoMergePolicy;
use tantivy::query::Weight;
use tantivy::schema::{
    FAST, Field, IndexRecordOption, Schema, TantivyDocument, TextFieldIndexing, TextOptions,
};
use tantivy::{
    Directory, DocId, Index, IndexReader, IndexWriter, ReloadPolicy, Score, Searcher, SegmentReader,
};

/// tantivy, as moraine pins it: custom tokenizers are tantivy `TextAnalyzer`s.
pub use tantivy;
pub use tokenizer::Tokenizers;

use super::bundle::{BundleView, SectionSink};
use super::kind::{
    BuildCtx, BuildSummary, ExactScorer, FileIndex, Hit, IndexBuilder, IndexKind, Order, Query,
};
use super::spec::IndexSpec;
use super::vector::{uint, with_defaults};
use crate::error::{Error, Result};

/// The kind's name in specs and bundle headers.
pub(crate) const KIND: &str = "fts";
/// The fast field holding each document's row offset.
pub(crate) const ROW_FIELD: &str = "_row";
/// Files below this many rows are indexed in memory when searched (the design default).
pub(crate) const DEFAULT_MIN_ROWS_FOR_SIDECAR: u64 = 1_000;
/// Longer values are truncated for indexing only (the design default; case F6).
pub(crate) const DEFAULT_MAX_INDEXED_CHARS: u64 = 1_000_000;
/// Memory of the writer of one build; tantivy's floor is 15 MB per thread.
const WRITER_HEAP: usize = 32 << 20;
/// Build memory per row assumed when sizing parts (a part is one segment).
const BYTES_PER_ROW: usize = 2048;

/// How much a field records (`Basic | Freq | Positions`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Record {
    /// Documents only: a term counts once per document.
    Basic,
    /// Term frequencies.
    Freqs,
    /// Frequencies and positions; needed by phrase queries.
    Positions,
}

impl Record {
    fn parse(s: &str) -> Result<Record> {
        match s {
            "basic" => Ok(Record::Basic),
            "freqs" => Ok(Record::Freqs),
            "positions" => Ok(Record::Positions),
            other => Err(Error::InvalidIndexSpec(format!(
                "unknown record {other:?} (basic, freqs, positions)"
            ))),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Record::Basic => "basic",
            Record::Freqs => "freqs",
            Record::Positions => "positions",
        }
    }

    fn option(self) -> IndexRecordOption {
        match self {
            Record::Basic => IndexRecordOption::Basic,
            Record::Freqs => IndexRecordOption::WithFreqs,
            Record::Positions => IndexRecordOption::WithFreqsAndPositions,
        }
    }
}

/// One indexed column.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TextField {
    /// The table column.
    pub column: String,
    /// The registered tokenizer that splits it.
    pub tokenizer: String,
    /// What the index records per term.
    pub record: Record,
}

/// The hashed parameters of a resolved spec.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FtsParams {
    /// The indexed columns, in spec order.
    pub fields: Vec<TextField>,
    /// Characters of a value indexed at most; the rest is ignored.
    pub max_indexed_chars: usize,
}

impl FtsParams {
    /// Reads them from a resolved spec.
    pub fn of(spec: &IndexSpec) -> Result<FtsParams> {
        let bad = |m: String| Error::InvalidIndexSpec(format!("index {}: {m}", spec.name));
        let fields = spec
            .params
            .get("fields")
            .and_then(Value::as_object)
            .ok_or_else(|| bad("params.fields is missing".into()))?;
        let mut out = Vec::with_capacity(spec.columns.len());
        for c in &spec.columns {
            let f = fields
                .get(c)
                .ok_or_else(|| bad(format!("params.fields has no entry for {c}")))?;
            let tokenizer = f
                .get("tokenizer")
                .and_then(Value::as_str)
                .ok_or_else(|| bad(format!("params.fields.{c}.tokenizer is missing")))?;
            let record = Record::parse(
                f.get("record")
                    .and_then(Value::as_str)
                    .ok_or_else(|| bad(format!("params.fields.{c}.record is missing")))?,
            )?;
            out.push(TextField {
                column: c.clone(),
                tokenizer: tokenizer.to_string(),
                record,
            });
        }
        let max = uint(&spec.params, "max_indexed_chars", DEFAULT_MAX_INDEXED_CHARS);
        Ok(FtsParams {
            fields: out,
            max_indexed_chars: usize::try_from(max).unwrap_or(usize::MAX).max(1),
        })
    }

    /// The tantivy schema: one text field per column, in spec order, then `_row`.
    pub fn schema(&self) -> (Schema, Vec<Field>, Field) {
        let mut b = Schema::builder();
        let fields = self
            .fields
            .iter()
            .map(|f| {
                let indexing = TextFieldIndexing::default()
                    .set_tokenizer(&f.tokenizer)
                    .set_index_option(f.record.option());
                b.add_text_field(
                    &f.column,
                    TextOptions::default().set_indexing_options(indexing),
                )
            })
            .collect();
        let row = b.add_u64_field(ROW_FIELD, FAST);
        (b.build(), fields, row)
    }

    fn tokenizer_names(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().map(|f| f.tokenizer.as_str())
    }
}

/// See the module documentation.
#[derive(Debug, Clone)]
pub struct FtsKind {
    tokenizers: Arc<Tokenizers>,
}

impl FtsKind {
    /// The kind, resolving tokenizer names in `tokenizers`.
    pub fn new(tokenizers: Arc<Tokenizers>) -> FtsKind {
        FtsKind { tokenizers }
    }

    /// The tokenizer registry this kind resolves names in.
    pub fn tokenizers(&self) -> &Arc<Tokenizers> {
        &self.tokenizers
    }

    /// A builder that keeps its segments in memory (for files searched without a sidecar).
    pub(crate) fn memory_builder(&self, spec: &IndexSpec) -> Result<FtsBuilder> {
        FtsBuilder::new(self, spec, None)
    }
}

fn native(e: impl std::fmt::Display) -> Error {
    Error::Internal(format!("tantivy: {e}"))
}

/// tantivy's version and index format, e.g. `tantivy-0.27-f7`.
fn fingerprint() -> String {
    let v = tantivy::version_string();
    let num = v
        .split_whitespace()
        .nth(1)
        .unwrap_or("v0.0.0")
        .trim_start_matches('v')
        .trim_end_matches(',');
    let mm: Vec<&str> = num.split('.').take(2).collect();
    format!(
        "tantivy-{}-f{}",
        mm.join("."),
        tantivy::INDEX_FORMAT_VERSION
    )
}

/// Column names the query syntax reads as names: no `:`, `.`, `^`, quotes, parentheses,
/// whitespace or leading `-` or `_`.
fn valid_field_name(c: &str) -> bool {
    !c.is_empty()
        && !c.starts_with(['-', '_', '+'])
        && !c
            .chars()
            .any(|ch| ch.is_whitespace() || ":.^\"'()[]{}~*?\\/!".contains(ch))
}

impl IndexKind for FtsKind {
    fn name(&self) -> &'static str {
        KIND
    }

    fn format_version(&self) -> u32 {
        1
    }

    fn library_fingerprint(&self) -> String {
        fingerprint()
    }

    fn order(&self) -> Order {
        Order::Descending
    }

    /// Accepts `params.tokenizer` and `params.record` as defaults for every column, and
    /// `params.fields.<column>` for one; stores every column's settings in `params.fields`.
    fn resolve(&self, spec: &IndexSpec, schema: &ArrowSchema) -> Result<IndexSpec> {
        let bad = |m: String| Error::InvalidIndexSpec(format!("index {}: {m}", spec.name));
        if spec.columns.is_empty() {
            return Err(bad("a full-text index needs at least one column".into()));
        }
        let mut seen = std::collections::HashSet::new();
        for c in &spec.columns {
            if !seen.insert(c) {
                return Err(bad(format!("column {c} named twice")));
            }
            let f = schema
                .field_with_name(c)
                .map_err(|_| bad(format!("no column {c}")))?;
            if !matches!(
                f.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                return Err(bad(format!(
                    "column {c} has type {}; a full-text column is a string",
                    f.data_type()
                )));
            }
            if !valid_field_name(c) {
                return Err(bad(format!(
                    "column name {c:?} cannot be named in a query; full-text columns need names \
                     without whitespace, punctuation of the query syntax, or a leading -, _ or +"
                )));
            }
        }
        let p = match &spec.params {
            Value::Object(m) => m.clone(),
            Value::Null => serde_json::Map::new(),
            _ => return Err(bad("params must be an object".into())),
        };
        for k in p.keys() {
            if !["tokenizer", "record", "fields", "max_indexed_chars"].contains(&k.as_str()) {
                return Err(bad(format!("unknown parameter {k:?}")));
            }
        }
        let tokenizer = p
            .get("tokenizer")
            .and_then(Value::as_str)
            .unwrap_or("default");
        let record = p
            .get("record")
            .and_then(Value::as_str)
            .unwrap_or("positions");
        let given = match p.get("fields") {
            None | Some(Value::Null) => serde_json::Map::new(),
            Some(Value::Object(m)) => m.clone(),
            Some(_) => return Err(bad("params.fields must be an object".into())),
        };
        for k in given.keys() {
            if !spec.columns.contains(k) {
                return Err(bad(format!(
                    "params.fields names {k}, which is not indexed"
                )));
            }
        }
        let mut fields = serde_json::Map::new();
        for c in &spec.columns {
            let f = given.get(c).cloned().unwrap_or(Value::Null);
            let f = match f {
                Value::Object(m) => m,
                Value::Null => serde_json::Map::new(),
                _ => return Err(bad(format!("params.fields.{c} must be an object"))),
            };
            for k in f.keys() {
                if !["tokenizer", "record"].contains(&k.as_str()) {
                    return Err(bad(format!("unknown parameter fields.{c}.{k}")));
                }
            }
            let t = f
                .get("tokenizer")
                .and_then(Value::as_str)
                .unwrap_or(tokenizer);
            if !self.tokenizers.knows(t) {
                return Err(Error::TokenizerNotRegistered(self.tokenizers.not_found(t)));
            }
            let r = Record::parse(f.get("record").and_then(Value::as_str).unwrap_or(record))?;
            fields.insert(c.clone(), json!({"tokenizer": t, "record": r.name()}));
        }
        let max = p
            .get("max_indexed_chars")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_MAX_INDEXED_CHARS);
        if max == 0 {
            return Err(bad("max_indexed_chars must be positive".into()));
        }
        let mut s = spec.clone();
        s.params = json!({"fields": fields, "max_indexed_chars": max});
        // Boosts and the default operator change no sidecar: they are search defaults.
        let boosts: BTreeMap<String, Value> = spec
            .columns
            .iter()
            .map(|c| (c.clone(), json!(1.0)))
            .collect();
        s.search = with_defaults(&s.search, json!({"boosts": boosts, "conjunction": false}));
        check_search(&s.search, &spec.columns).map_err(|e| bad(e.to_string()))?;
        s.policy = with_defaults(
            &s.policy,
            json!({"min_rows_for_sidecar": DEFAULT_MIN_ROWS_FOR_SIDECAR}),
        );
        FtsParams::of(&s)?;
        Ok(s)
    }

    fn wants_sidecar(&self, spec: &IndexSpec, file_rows: u64) -> bool {
        file_rows
            >= uint(
                &spec.policy,
                "min_rows_for_sidecar",
                DEFAULT_MIN_ROWS_FOR_SIDECAR,
            )
    }

    fn build_bytes_per_row(&self, _spec: &IndexSpec) -> usize {
        BYTES_PER_ROW
    }

    fn builder(&self, spec: &IndexSpec, _ctx: &BuildCtx) -> Result<Box<dyn IndexBuilder>> {
        let dir = std::env::temp_dir().join(format!(
            "moraine-fts-{}-{}",
            std::process::id(),
            BUILD_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        Ok(Box::new(FtsBuilder::new(self, spec, Some(dir))?))
    }

    fn open(&self, spec: &IndexSpec, bundle: BundleView) -> Result<Arc<dyn FileIndex>> {
        Ok(Arc::new(FtsIndex::open(self, spec, bundle)?))
    }

    fn exact(&self, _spec: &IndexSpec, _q: &Query<'_>) -> Result<Box<dyn ExactScorer>> {
        Err(not_text_query())
    }

    fn validate(&self, _spec: &IndexSpec, _q: &Query<'_>) -> Result<()> {
        Err(not_text_query())
    }
}

fn not_text_query() -> Error {
    Error::InvalidQuery("a full-text index is searched with search_text".into())
}

/// Checks search defaults or overrides: `boosts` maps indexed columns to finite, non-negative
/// numbers; `conjunction` is a boolean.
pub(crate) fn check_search(search: &Value, columns: &[String]) -> Result<()> {
    if let Some(b) = search.get("boosts") {
        let m = b
            .as_object()
            .ok_or_else(|| Error::InvalidQuery("boosts must be an object".into()))?;
        for (c, v) in m {
            if !columns.contains(c) {
                return Err(Error::InvalidQuery(format!(
                    "boost for {c}, which is not indexed"
                )));
            }
            if !v.as_f64().is_some_and(|x| x.is_finite() && x >= 0.0) {
                return Err(Error::InvalidQuery(format!(
                    "boost for {c} must be a non-negative number"
                )));
            }
        }
    }
    if search.get("conjunction").is_some_and(|c| !c.is_boolean()) {
        return Err(Error::InvalidQuery(
            "conjunction must be true or false".into(),
        ));
    }
    Ok(())
}

static BUILD_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The text of a string column, row by row.
enum Texts<'a> {
    Utf8(&'a StringArray),
    Large(&'a LargeStringArray),
    View(&'a StringViewArray),
}

impl<'a> Texts<'a> {
    fn of(a: &'a dyn Array) -> Result<Texts<'a>> {
        let any = a.as_any();
        if let Some(s) = any.downcast_ref::<StringArray>() {
            Ok(Texts::Utf8(s))
        } else if let Some(s) = any.downcast_ref::<LargeStringArray>() {
            Ok(Texts::Large(s))
        } else if let Some(s) = any.downcast_ref::<StringViewArray>() {
            Ok(Texts::View(s))
        } else {
            Err(Error::Internal(format!(
                "fts: column of type {} is not a string",
                a.data_type()
            )))
        }
    }

    fn get(&self, i: usize) -> Option<&'a str> {
        match self {
            Texts::Utf8(s) => (!s.is_null(i)).then(|| s.value(i)),
            Texts::Large(s) => (!s.is_null(i)).then(|| s.value(i)),
            Texts::View(s) => (!s.is_null(i)).then(|| s.value(i)),
        }
    }
}

/// `s` cut to at most `max` characters (case F6).
fn truncate(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

/// Builds one file's index. See the module documentation.
pub(crate) struct FtsBuilder {
    params: FtsParams,
    fields: Vec<Field>,
    row: Field,
    index: Index,
    writer: Option<IndexWriter<TantivyDocument>>,
    /// The segments to merge, in row order (filled when the build finishes).
    parts: Vec<tantivy::index::SegmentId>,
    /// Parts committed so far.
    commits: u32,
    next_row: u64,
    indexed: u64,
    /// The temporary directory of a disk build, removed on drop.
    dir: Option<PathBuf>,
}

impl FtsBuilder {
    fn new(kind: &FtsKind, spec: &IndexSpec, dir: Option<PathBuf>) -> Result<FtsBuilder> {
        let params = FtsParams::of(spec)?;
        let (schema, fields, row) = params.schema();
        let mut index = match &dir {
            Some(d) => {
                std::fs::create_dir_all(d)?;
                let md = MmapDirectory::open(d).map_err(native)?;
                Index::create(md, schema, tantivy::IndexSettings::default()).map_err(native)?
            }
            None => Index::create_in_ram(schema),
        };
        index.set_tokenizers(kind.tokenizers.manager(params.tokenizer_names())?);
        let writer: IndexWriter<TantivyDocument> = index
            .writer_with_num_threads(1, WRITER_HEAP)
            .map_err(native)?;
        writer.set_merge_policy(Box::new(NoMergePolicy));
        Ok(FtsBuilder {
            params,
            fields,
            row,
            index,
            writer: Some(writer),
            parts: Vec::new(),
            commits: 0,
            next_row: 0,
            indexed: 0,
            dir,
        })
    }

    fn writer(&mut self) -> Result<&mut IndexWriter<TantivyDocument>> {
        self.writer
            .as_mut()
            .ok_or_else(|| Error::Internal("fts: builder already finished".into()))
    }

    fn empty_doc(&self, row: u64) -> TantivyDocument {
        let mut d = TantivyDocument::default();
        d.add_u64(self.row, row);
        d
    }

    /// Commits what was added since the last part as a new segment.
    /// A commit may write several segments: tantivy also flushes whenever its writer memory
    /// is full.
    fn commit_part(&mut self) -> Result<()> {
        self.writer()?.commit().map_err(native)?;
        self.commits += 1;
        Ok(())
    }

    /// The committed segments, in row order (by the `_row` of their first document; segment
    /// ids are random).
    fn segments_in_row_order(&self) -> Result<Vec<tantivy::index::SegmentId>> {
        let reader: IndexReader = self
            .index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(native)?;
        let searcher = reader.searcher();
        let mut segs = Vec::new();
        for seg in searcher.segment_readers() {
            let rows = seg.fast_fields().u64(ROW_FIELD).map_err(native)?;
            segs.push((rows.first(0).unwrap_or(u64::MAX), seg.segment_id()));
        }
        segs.sort_by_key(|s| s.0);
        Ok(segs.into_iter().map(|s| s.1).collect())
    }

    /// Commits, merges the parts in row order into one segment, and returns the index.
    fn finish_index(&mut self) -> Result<()> {
        self.commit_part()?;
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| Error::Internal("fts: builder already finished".into()))?;
        self.parts = self.segments_in_row_order()?;
        if self.parts.len() > 1 {
            writer.merge(&self.parts).wait().map_err(native)?;
        }
        writer.garbage_collect_files().wait().map_err(native)?;
        writer.wait_merging_threads().map_err(native)?;
        let metas = self.index.searchable_segment_metas().map_err(native)?;
        if metas.len() > 1 {
            return Err(Error::Internal(
                "fts: the merge left several segments".into(),
            ));
        }
        let docs = metas.first().map(|m| u64::from(m.max_doc())).unwrap_or(0);
        if docs != self.next_row {
            return Err(Error::Internal(format!(
                "fts: {docs} documents for {} rows",
                self.next_row
            )));
        }
        check_row_order(&self.index)
    }
}

/// Every document id equals its `_row`: the search routes rely on it to rank ties by row.
fn check_row_order(index: &Index) -> Result<()> {
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .map_err(native)?;
    let searcher = reader.searcher();
    for seg in searcher.segment_readers() {
        let rows = seg.fast_fields().u64(ROW_FIELD).map_err(native)?;
        for d in 0..seg.max_doc() {
            if rows.first(d) != Some(u64::from(d)) {
                return Err(Error::Internal(format!(
                    "fts: document {d} holds row {:?}",
                    rows.first(d)
                )));
            }
        }
    }
    Ok(())
}

/// Writes `index`'s one segment into `sink`: `meta.json` with the fixed segment id, and
/// `seg.<ext>` per component.
///
/// The files are copied as stored, footers included: `disk` names the directory of a disk
/// build (streamed from there); an in-memory build is read through tantivy's directory, whose
/// raw `atomic_read` keeps the footer that `open_read` strips.
fn pack(index: &Index, disk: Option<&std::path::Path>, sink: &mut dyn SectionSink) -> Result<()> {
    let metas = index.searchable_segment_metas().map_err(native)?;
    let dir = index.directory();
    let raw = dir
        .atomic_read(std::path::Path::new(dir::META))
        .map_err(native)?;
    let mut meta: Value = serde_json::from_slice(&raw)
        .map_err(|e| Error::Internal(format!("fts: meta.json: {e}")))?;
    if let Some(segs) = meta.get_mut("segments").and_then(Value::as_array_mut) {
        for s in segs {
            if let Some(Value::String(id)) = s.get_mut("segment_id") {
                *id = id
                    .chars()
                    .map(|c| if c == '-' { '-' } else { '0' })
                    .collect();
            }
        }
    }
    sink.section(dir::META)?
        .write_all(serde_json::to_string(&meta).unwrap_or_default().as_bytes())?;
    for m in &metas {
        for c in [
            SegmentComponent::Postings,
            SegmentComponent::Positions,
            SegmentComponent::Terms,
            SegmentComponent::Store,
            SegmentComponent::FastFields,
            SegmentComponent::FieldNorms,
        ] {
            let path = m.relative_path(c);
            if !dir.exists(&path).map_err(native)? {
                continue;
            }
            let name = path.to_string_lossy();
            let ext = name.split_once('.').map(|(_, e)| e).unwrap_or_default();
            let out = sink.section(&format!("{}{ext}", dir::SEG))?;
            match disk {
                Some(d) => {
                    let mut f = std::fs::File::open(d.join(&path))?;
                    std::io::copy(&mut f, out)?;
                }
                None => out.write_all(&dir.atomic_read(&path).map_err(native)?)?,
            }
        }
    }
    Ok(())
}

impl IndexBuilder for FtsBuilder {
    fn add(&mut self, first_row: u64, batch: &RecordBatch) -> Result<()> {
        // Rows the driver skipped (none today) become empty documents, so ids stay offsets.
        while self.next_row < first_row {
            let d = self.empty_doc(self.next_row);
            self.writer()?.add_document(d).map_err(native)?;
            self.next_row += 1;
        }
        if first_row < self.next_row {
            return Err(Error::Internal(format!(
                "fts: row {first_row} added after row {}",
                self.next_row
            )));
        }
        let cols: Vec<Texts<'_>> = (0..self.fields.len())
            .map(|i| Texts::of(batch.column(i).as_ref()))
            .collect::<Result<_>>()?;
        let max = self.params.max_indexed_chars;
        for r in 0..batch.num_rows() {
            let mut d = self.empty_doc(self.next_row);
            let mut any = false;
            for (f, c) in self.fields.iter().zip(&cols) {
                if let Some(t) = c.get(r) {
                    any |= !t.is_empty();
                    d.add_text(*f, truncate(t, max));
                }
            }
            self.indexed += u64::from(any);
            self.writer()?.add_document(d).map_err(native)?;
            self.next_row += 1;
        }
        Ok(())
    }

    fn memory_bytes(&self) -> usize {
        WRITER_HEAP
    }

    fn seal_part(&mut self, _sink: &mut dyn SectionSink) -> Result<()> {
        // A part is a committed segment on disk; the sections are written at the end.
        self.commit_part()
    }

    fn finish(self: Box<Self>, sink: &mut dyn SectionSink) -> Result<BuildSummary> {
        let mut me = *self;
        me.finish_index()?;
        pack(&me.index, me.dir.as_deref(), sink)?;
        Ok(BuildSummary {
            indexed_rows: me.indexed,
            parts: me.commits.max(1),
        })
    }
}

impl Drop for FtsBuilder {
    fn drop(&mut self) {
        if let Some(w) = self.writer.take() {
            let _ = w.wait_merging_threads();
        }
        if let Some(d) = &self.dir {
            let _ = std::fs::remove_dir_all(d);
        }
    }
}

/// An opened full-text sidecar (or an index built in memory for a file without one).
#[doc(hidden)]
pub struct FtsIndex {
    searcher: Searcher,
    _reader: IndexReader,
    /// Documents (the source file's rows).
    docs: u64,
    bytes: usize,
}

impl FtsIndex {
    fn open(kind: &FtsKind, spec: &IndexSpec, bundle: BundleView) -> Result<FtsIndex> {
        let params = FtsParams::of(spec)?;
        let bytes = bundle.len() as usize;
        let rows = bundle.header().source_rows;
        let mut index = Index::open(dir::BundleDirectory::new(&bundle)).map_err(native)?;
        // Searching needs no tokenizer (queries arrive parsed), but an unknown name is refused
        // here, as case F3 asks.
        index.set_tokenizers(kind.tokenizers.manager(params.tokenizer_names())?);
        let (expect, _, _) = params.schema();
        if index.schema() != expect {
            return Err(Error::Internal(
                "fts: the bundle's schema differs from the spec".into(),
            ));
        }
        let reader: IndexReader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(native)?;
        let searcher = reader.searcher();
        let docs = searcher.num_docs();
        if searcher.segment_readers().len() > 1 || docs != rows {
            return Err(Error::Internal(format!(
                "fts: {docs} documents in {} segments for {rows} rows",
                searcher.segment_readers().len()
            )));
        }
        Ok(FtsIndex {
            searcher,
            _reader: reader,
            docs,
            bytes,
        })
    }

    /// The tantivy searcher over the bundle's segment (with the file's own statistics).
    pub fn searcher(&self) -> &Searcher {
        &self.searcher
    }

    /// Documents, one per row of the source file.
    pub fn docs(&self) -> u64 {
        self.docs
    }

    /// Bytes of the bundle behind it.
    pub fn bundle_bytes(&self) -> usize {
        self.bytes
    }
}

impl FileIndex for FtsIndex {
    /// Term dictionaries, postings and fast fields stay in the mapped bundle; tantivy keeps
    /// small per-segment structures on the heap.
    fn resident_bytes(&self) -> usize {
        64 << 10
    }

    fn search(&self, _q: &Query<'_>, _a: Option<&RoaringBitmap>, _k: usize) -> Result<Vec<Hit>> {
        Err(not_text_query())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Collects the best `want` documents by (score descending, document id ascending), among
/// those `allow` admits, skipping blocks that cannot beat the current `want`-th score.
pub(crate) struct TopRows {
    pub want: usize,
    pub allow: Option<Arc<RoaringBitmap>>,
}

/// A document in the heap, ordered so that the worst is the greatest.
#[derive(Clone, Copy, PartialEq)]
struct Worst(Score, DocId);

impl Eq for Worst {}

impl PartialOrd for Worst {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}

impl Ord for Worst {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        o.0.total_cmp(&self.0).then(self.1.cmp(&o.1))
    }
}

pub(crate) struct TopRowsSegment;

impl SegmentCollector for TopRowsSegment {
    type Fruit = Vec<(Score, DocId)>;
    fn collect(&mut self, _doc: DocId, _score: Score) {}
    fn harvest(self) -> Self::Fruit {
        Vec::new()
    }
}

impl Collector for TopRows {
    type Fruit = Vec<(Score, DocId)>;
    type Child = TopRowsSegment;

    fn for_segment(&self, _ord: u32, _r: &SegmentReader) -> tantivy::Result<TopRowsSegment> {
        Ok(TopRowsSegment)
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, fruits: Vec<Self::Fruit>) -> tantivy::Result<Self::Fruit> {
        let mut all: Vec<(Score, DocId)> = fruits.into_iter().flatten().collect();
        all.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        all.truncate(self.want);
        Ok(all)
    }

    fn collect_segment(
        &self,
        weight: &dyn Weight,
        _ord: u32,
        reader: &SegmentReader,
    ) -> tantivy::Result<Self::Fruit> {
        let want = self.want;
        let mut heap: std::collections::BinaryHeap<Worst> =
            std::collections::BinaryHeap::with_capacity(want + 1);
        if want == 0 {
            return Ok(Vec::new());
        }
        let allow = self.allow.as_deref();
        // Documents arrive in id order and only beat the threshold when strictly better, so
        // a later document never displaces an equal-scoring earlier one: ties go to the lower
        // row, as on every route.
        weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
            if allow.is_none_or(|a| a.contains(doc)) {
                heap.push(Worst(score, doc));
                if heap.len() > want {
                    heap.pop();
                }
            }
            if heap.len() == want {
                heap.peek().map(|w| w.0).unwrap_or(Score::MIN)
            } else {
                Score::MIN
            }
        })?;
        let mut v: Vec<(Score, DocId)> = heap.into_iter().map(|w| (w.0, w.1)).collect();
        v.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::bundle::{BundleBytes, BundleMeta, BundleWriter};
    use arrow::datatypes::Field as AField;

    /// A part larger than the writer's memory is flushed as several segments by one commit;
    /// they are merged in row order (found by the medium benchmark).
    #[test]
    fn one_commit_of_several_segments() {
        let kind = FtsKind::new(Arc::new(Tokenizers::default()));
        let schema = ArrowSchema::new(vec![AField::new("body", DataType::Utf8, true)]);
        let spec = kind
            .resolve(&IndexSpec::new("f", "fts", &["body"]), &schema)
            .unwrap();
        let mut b = kind.memory_builder(&spec).unwrap();
        let mut x = 7u64;
        let rows = 120_000;
        let texts: Vec<String> = (0..rows)
            .map(|_| {
                (0..60)
                    .map(|_| {
                        x = x
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        format!("w{}", (x >> 33) % 50_000)
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect();
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from_iter_values(texts.iter()))],
        )
        .unwrap();
        b.add(0, &batch).unwrap();
        b.commit_part().unwrap();
        assert!(
            b.segments_in_row_order().unwrap().len() > 1,
            "one segment only"
        );
        b.finish_index().unwrap();
        assert_eq!(b.index.searchable_segment_ids().unwrap().len(), 1);
    }

    #[test]
    fn memory_build_round_trip() {
        let kind = FtsKind::new(Arc::new(Tokenizers::default()));
        let schema = ArrowSchema::new(vec![AField::new("body", DataType::Utf8, true)]);
        let spec = kind
            .resolve(&IndexSpec::new("f", "fts", &["body"]), &schema)
            .unwrap();
        let mut b = kind.memory_builder(&spec).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(vec![
                Some("a b"),
                None,
                Some("b c"),
            ]))],
        )
        .unwrap();
        b.add(0, &batch).unwrap();
        let meta = BundleMeta {
            kind: KIND.into(),
            kind_format: 1,
            spec_hash: "0123456789abcdef".into(),
            source_rows: 3,
            source_size: 1,
        };
        let mut w = BundleWriter::new(std::io::Cursor::new(Vec::new()), meta).unwrap();
        IndexBuilder::finish(Box::new(b), &mut w).unwrap();
        let (c, _) = w.finish(2).unwrap();
        let view = BundleView::parse(BundleBytes::copy(&c.into_inner()).unwrap()).unwrap();
        let i = kind.open(&spec, view).unwrap();
        assert!(i.as_any().downcast_ref::<FtsIndex>().is_some());
    }
}
