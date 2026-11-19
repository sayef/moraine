//! The key-index sidecar (D19): per data file, a binary fuse filter over primary-key
//! fingerprints, so files that merely overlap a key's range are rejected without a read; the
//! first key of every page for files sorted by the primary key; for files that are not, sorted
//! (fingerprint, row offset) pairs; and for a declared cluster key, the row range of each
//! distinct value.
//!
//! Kind `key`, one implicit index per table with a primary key (`_moraine_key`, not in
//! `moraine.indexes`). Its spec is derived from `moraine.primaryKey`, `moraine.clusterBy` and the
//! page row limit, so its hash changes with them.
//!
//! | Section | Contents |
//! |---|---|
//! | `fuse` | `u64` key count, the filter's 20-byte descriptor (seed `u64`, segment length, mask, segment count length, `u32` each, little-endian), the 8-bit fingerprints |
//! | `pages` | `u32` count, then per page `u32` first row and `u32` key length, then the keys |
//! | `pairs` | `u64` count, then `(u64 fingerprint, u32 row)` sorted |
//! | `cluster` | `u32` count, then per value `u32` start, `u32` end, `u32` length, then the values |

pub mod encode;

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::Schema;
use roaring::RoaringBitmap;
use serde_json::{Value, json};
use xorf::{BinaryFuse8, BinaryFuse8Ref, DmaSerializable, Filter, FilterRef};

use super::bundle::{BundleView, SectionSink};
use super::kind::{
    BuildCtx, BuildSummary, ExactScorer, FileIndex, Hit, IndexBuilder, IndexKind, Order, Query,
};
use super::spec::IndexSpec;
use crate::error::{Error, Result};
use crate::options::TableDef;
use encode::{encode_row, fingerprint};

/// The name of the implicit key index.
pub const KEY_INDEX: &str = "_moraine_key";
/// Files written with at least this many rows get their key index from the writer (D19).
pub const INLINE_MIN_ROWS: u64 = 100_000;

/// The key index's spec for a table, `None` without a primary key.
pub fn key_spec(def: &TableDef) -> Option<IndexSpec> {
    if def.primary_key.is_empty() {
        return None;
    }
    let mut columns = def.primary_key.clone();
    for c in &def.cluster_by {
        if !columns.contains(c) {
            columns.push(c.clone());
        }
    }
    let cols: Vec<&str> = columns.iter().map(String::as_str).collect();
    Some(IndexSpec::new(KEY_INDEX, "key", &cols).params(json!({
        "primary_key": def.primary_key,
        "cluster": def.cluster_by,
        "page_rows": def.page_row_limit,
    })))
}

struct Params {
    pk: usize,
    /// Positions of the cluster columns among the spec's columns.
    cluster: Vec<usize>,
    page_rows: u64,
}

impl Params {
    fn of(spec: &IndexSpec) -> Result<Params> {
        let bad = || Error::InvalidIndexSpec(format!("key index {}: bad params", spec.name));
        let list = |k: &str| -> Result<Vec<String>> {
            spec.params
                .get(k)
                .and_then(Value::as_array)
                .ok_or_else(bad)?
                .iter()
                .map(|v| v.as_str().map(String::from).ok_or_else(bad))
                .collect()
        };
        let pk = list("primary_key")?;
        let cluster = list("cluster")?
            .iter()
            .map(|c| spec.columns.iter().position(|x| x == c).ok_or_else(bad))
            .collect::<Result<_>>()?;
        if pk.is_empty() || spec.columns[..pk.len().min(spec.columns.len())] != pk[..] {
            return Err(bad());
        }
        Ok(Params {
            pk: pk.len(),
            cluster,
            page_rows: spec
                .params
                .get("page_rows")
                .and_then(Value::as_u64)
                .filter(|n| *n > 0)
                .ok_or_else(bad)?,
        })
    }
}

/// See the module documentation.
#[derive(Debug, Default)]
pub struct KeyKind;

impl IndexKind for KeyKind {
    fn name(&self) -> &'static str {
        "key"
    }

    fn format_version(&self) -> u32 {
        1
    }

    /// xorf's binary fuse construction and moraine's key encoding (pinned by tests).
    fn library_fingerprint(&self) -> String {
        "xorf-bfuse8-0.13;keys-1".into()
    }

    fn order(&self) -> Order {
        Order::Ascending
    }

    fn resolve(&self, spec: &IndexSpec, schema: &Schema) -> Result<IndexSpec> {
        Params::of(spec)?;
        for c in &spec.columns {
            schema
                .field_with_name(c)
                .map_err(|_| Error::InvalidIndexSpec(format!("no column {c}")))?;
        }
        Ok(spec.clone())
    }

    fn wants_sidecar(&self, _spec: &IndexSpec, file_rows: u64) -> bool {
        file_rows > 0
    }

    fn build_bytes_per_row(&self, _spec: &IndexSpec) -> usize {
        // Fingerprint and offset per row; page and cluster keys are few.
        24
    }

    fn builder(&self, spec: &IndexSpec, _ctx: &BuildCtx) -> Result<Box<dyn IndexBuilder>> {
        Ok(Box::new(KeyBuilder {
            p: Params::of(spec)?,
            fps: Vec::new(),
            sorted: true,
            prev: None,
            pages: Vec::new(),
            clusters: Vec::new(),
            cluster_ok: true,
            rows: 0,
        }))
    }

    fn open(&self, spec: &IndexSpec, bundle: BundleView) -> Result<Arc<dyn FileIndex>> {
        Params::of(spec)?;
        Ok(Arc::new(KeyIndex::parse(&bundle)?))
    }

    fn exact(&self, _spec: &IndexSpec, _q: &Query<'_>) -> Result<Box<dyn ExactScorer>> {
        Err(Error::InvalidQuery(
            "the key index is used by key lookups, not searched".into(),
        ))
    }
}

struct KeyBuilder {
    p: Params,
    /// (fingerprint, row) of every row.
    fps: Vec<(u64, u32)>,
    /// Whether primary keys arrived in non-decreasing order.
    sorted: bool,
    prev: Option<Vec<u8>>,
    /// First row and primary key of every page.
    pages: Vec<(u32, Vec<u8>)>,
    /// Cluster value runs: value, first row, end row.
    clusters: Vec<(Vec<u8>, u32, u32)>,
    /// Whether every cluster value forms one contiguous run.
    cluster_ok: bool,
    rows: u64,
}

impl IndexBuilder for KeyBuilder {
    fn add(&mut self, first_row: u64, batch: &RecordBatch) -> Result<()> {
        let pk: Vec<ArrayRef> = batch.columns()[..self.p.pk].to_vec();
        let cl: Vec<ArrayRef> = self
            .p
            .cluster
            .iter()
            .map(|i| batch.column(*i).clone())
            .collect();
        for i in 0..batch.num_rows() {
            let row = u32::try_from(first_row + i as u64)
                .map_err(|_| Error::Internal("key index: row offset beyond u32".into()))?;
            let k = encode_row(&pk, i)?;
            self.fps.push((fingerprint(&k), row));
            if u64::from(row) % self.p.page_rows == 0 {
                self.pages.push((row, k.clone()));
            }
            if self.prev.as_ref().is_some_and(|p| *p > k) {
                self.sorted = false;
            }
            self.prev = Some(k);
            if !cl.is_empty() {
                let v = encode_row(&cl, i)?;
                match self.clusters.last_mut() {
                    Some(last) if last.0 == v => last.2 = row + 1,
                    Some(last) if last.0 > v => {
                        self.cluster_ok = false;
                        self.clusters.push((v, row, row + 1));
                    }
                    _ => self.clusters.push((v, row, row + 1)),
                }
            }
        }
        self.rows = first_row + batch.num_rows() as u64;
        Ok(())
    }

    fn memory_bytes(&self) -> usize {
        self.fps.capacity() * 16 + self.pages.len() * 48 + self.clusters.len() * 48
    }

    fn seal_part(&mut self, _sink: &mut dyn SectionSink) -> Result<()> {
        // One part: a key index is small (about 12 bytes per row while building).
        Ok(())
    }

    fn finish(mut self: Box<Self>, sink: &mut dyn SectionSink) -> Result<BuildSummary> {
        let mut keys: Vec<u64> = self.fps.iter().map(|(f, _)| *f).collect();
        keys.sort_unstable();
        keys.dedup();
        if !keys.is_empty() {
            let f = BinaryFuse8::try_from(&keys)
                .map_err(|e| Error::Internal(format!("key index filter: {e}")))?;
            // The descriptor in xorf's documented layout (verified against its source: seed
            // u64, segment length, mask and segment count length u32, little-endian), written
            // through `DmaSerializable` rather than serde.
            let mut d = [0u8; BinaryFuse8::DESCRIPTOR_LEN];
            f.dma_copy_descriptor_to(&mut d);
            let w = sink.section("fuse")?;
            w.write_all(&(keys.len() as u64).to_le_bytes())?;
            w.write_all(&d)?;
            w.write_all(f.dma_fingerprints())?;
        }
        if self.sorted && self.p.cluster.is_empty() {
            let w = sink.section("pages")?;
            w.write_all(&(self.pages.len() as u32).to_le_bytes())?;
            let mut head = Vec::with_capacity(self.pages.len() * 8);
            for (row, k) in &self.pages {
                head.extend_from_slice(&row.to_le_bytes());
                head.extend_from_slice(&(k.len() as u32).to_le_bytes());
            }
            w.write_all(&head)?;
            for (_, k) in &self.pages {
                w.write_all(k)?;
            }
        } else {
            self.fps.sort_unstable();
            let w = sink.section("pairs")?;
            w.write_all(&(self.fps.len() as u64).to_le_bytes())?;
            let mut buf = Vec::with_capacity(self.fps.len() * 12);
            for (f, r) in &self.fps {
                buf.extend_from_slice(&f.to_le_bytes());
                buf.extend_from_slice(&r.to_le_bytes());
            }
            w.write_all(&buf)?;
        }
        if !self.p.cluster.is_empty() && self.cluster_ok {
            let w = sink.section("cluster")?;
            w.write_all(&(self.clusters.len() as u32).to_le_bytes())?;
            let mut head = Vec::with_capacity(self.clusters.len() * 12);
            for (v, s, e) in &self.clusters {
                head.extend_from_slice(&s.to_le_bytes());
                head.extend_from_slice(&e.to_le_bytes());
                head.extend_from_slice(&(v.len() as u32).to_le_bytes());
            }
            w.write_all(&head)?;
            for (v, ..) in &self.clusters {
                w.write_all(v)?;
            }
        }
        Ok(BuildSummary {
            indexed_rows: self.fps.len() as u64,
            parts: 1,
        })
    }
}

/// Where a key may be in a file, per its key index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyRows {
    /// The filter proves the key absent.
    Absent,
    /// These rows may hold it (a page range, or the rows whose fingerprint matches).
    Rows(Vec<Range<u64>>),
}

/// An opened key index.
pub struct KeyIndex {
    /// The filter's descriptor and fingerprints, checked on open.
    filter: Option<(Vec<u8>, Vec<u8>)>,
    pages: Option<(Vec<u32>, Vec<Vec<u8>>)>,
    pairs: Option<Vec<(u64, u32)>>,
    clusters: Option<Vec<(Vec<u8>, u32, u32)>>,
    rows: u64,
    indexed: u64,
}

fn u32_at(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

fn u64_at(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?))
}

/// `count` entries of `entry` header bytes, each ending in a key length, then the keys.
fn keyed(b: &[u8], entry: usize) -> Option<Vec<(Vec<u32>, Vec<u8>)>> {
    let n = u32_at(b, 0)? as usize;
    let head = 4usize.checked_add(n.checked_mul(entry)?)?;
    let mut at = head;
    let mut out = Vec::with_capacity(n.min(b.len()));
    for i in 0..n {
        let base = 4 + i * entry;
        let fields: Vec<u32> = (0..entry / 4)
            .map(|j| u32_at(b, base + 4 * j))
            .collect::<Option<_>>()?;
        let len = *fields.last()? as usize;
        let k = b.get(at..at.checked_add(len)?)?.to_vec();
        at += len;
        out.push((fields, k));
    }
    (at == b.len()).then_some(out)
}

impl KeyIndex {
    fn parse(v: &BundleView) -> Result<KeyIndex> {
        let bad = |s: &str| Error::Internal(format!("key index: corrupt {s} section"));
        let rows = v.header().source_rows;
        let indexed = v.header().indexed_rows;
        let filter = match v.section("fuse") {
            None => None,
            Some(b) => {
                let seed_end = 8 + BinaryFuse8::DESCRIPTOR_LEN;
                let desc = b.get(8..seed_end).ok_or_else(|| bad("fuse"))?;
                let len = u32_at(desc, 8).ok_or_else(|| bad("fuse"))?;
                let mask = u32_at(desc, 12).ok_or_else(|| bad("fuse"))?;
                let count = u32_at(desc, 16).ok_or_else(|| bad("fuse"))?;
                let fp = &b[seed_end..];
                // Lookups index fingerprints below segment_count_length + 2 segments; anything
                // else would read out of bounds.
                let need = u64::from(count) + 2 * u64::from(len);
                if !len.is_power_of_two()
                    || mask != len - 1
                    || count % len != 0
                    || (fp.len() as u64) < need
                {
                    return Err(bad("fuse"));
                }
                Some((desc.to_vec(), fp.to_vec()))
            }
        };
        let pages = match v.section("pages") {
            None => None,
            Some(b) => {
                let e = keyed(b, 8).ok_or_else(|| bad("pages"))?;
                let starts: Vec<u32> = e.iter().map(|(f, _)| f[0]).collect();
                let keys: Vec<Vec<u8>> = e.into_iter().map(|(_, k)| k).collect();
                if starts.windows(2).any(|w| w[0] >= w[1]) || keys.windows(2).any(|w| w[0] > w[1]) {
                    return Err(bad("pages"));
                }
                Some((starts, keys))
            }
        };
        let pairs = match v.section("pairs") {
            None => None,
            Some(b) => {
                let n = u64_at(b, 0).ok_or_else(|| bad("pairs"))? as usize;
                if n.checked_mul(12).and_then(|x| x.checked_add(8)) != Some(b.len()) {
                    return Err(bad("pairs"));
                }
                let p: Vec<(u64, u32)> = (0..n)
                    .map(|i| {
                        let at = 8 + 12 * i;
                        (u64_at(b, at).unwrap_or(0), u32_at(b, at + 8).unwrap_or(0))
                    })
                    .collect();
                if p.windows(2).any(|w| w[0] > w[1]) {
                    return Err(bad("pairs"));
                }
                Some(p)
            }
        };
        let clusters = match v.section("cluster") {
            None => None,
            Some(b) => Some(
                keyed(b, 12)
                    .ok_or_else(|| bad("cluster"))?
                    .into_iter()
                    .map(|(f, k)| (k, f[0], f[1]))
                    .collect(),
            ),
        };
        Ok(KeyIndex {
            filter,
            pages,
            pairs,
            clusters,
            rows,
            indexed,
        })
    }

    /// The rows that may hold the primary key with canonical bytes `key`.
    pub fn lookup(&self, key: &[u8]) -> KeyRows {
        let fp = fingerprint(key);
        match &self.filter {
            Some((d, f)) if !BinaryFuse8Ref::from_dma(d, f).contains(&fp) => {
                return KeyRows::Absent;
            }
            None if self.indexed == 0 => return KeyRows::Absent,
            _ => {}
        }
        if let Some((starts, keys)) = &self.pages {
            // Pages from the last one starting below the key to the last one starting at or
            // below it: a key repeated across a page boundary is covered.
            let le = keys.partition_point(|k| k.as_slice() <= key);
            let lt = keys.partition_point(|k| k.as_slice() < key);
            if le == 0 {
                return KeyRows::Absent;
            }
            let first = lt.saturating_sub(1);
            let start = u64::from(starts[first]);
            let end = starts.get(le).map_or(self.rows, |s| u64::from(*s));
            return KeyRows::Rows(std::iter::once(start..end).collect());
        }
        if let Some(p) = &self.pairs {
            let lo = p.partition_point(|x| x.0 < fp);
            let rows: Vec<Range<u64>> = p[lo..]
                .iter()
                .take_while(|x| x.0 == fp)
                .map(|x| u64::from(x.1)..u64::from(x.1) + 1)
                .collect();
            return if rows.is_empty() {
                KeyRows::Absent
            } else {
                KeyRows::Rows(rows)
            };
        }
        KeyRows::Rows(std::iter::once(0..self.rows).collect())
    }

    /// The rows holding cluster value `value` (canonical bytes), when the file has the
    /// cluster section; `Some(None)` when it has none of them.
    pub fn cluster_rows(&self, value: &[u8]) -> Option<Option<Range<u64>>> {
        let c = self.clusters.as_ref()?;
        Some(
            c.binary_search_by(|(v, ..)| v.as_slice().cmp(value))
                .ok()
                .map(|i| u64::from(c[i].1)..u64::from(c[i].2)),
        )
    }

    /// Whether the index holds page ranges (only key-sorted files have them, D36).
    pub fn has_pages(&self) -> bool {
        self.pages.is_some()
    }

    /// Whether the index holds key fingerprints with their row offsets.
    pub fn has_pairs(&self) -> bool {
        self.pairs.is_some()
    }
}

impl FileIndex for KeyIndex {
    fn resident_bytes(&self) -> usize {
        self.filter.as_ref().map_or(0, |f| f.1.len())
            + self.pairs.as_ref().map_or(0, |p| p.len() * 16)
            + self.pages.as_ref().map_or(0, |(s, k)| {
                s.len() * 4 + k.iter().map(Vec::len).sum::<usize>()
            })
            + 4096
    }

    /// Candidate rows of each key, as hits with score 0.
    fn search(&self, q: &Query<'_>, allow: Option<&RoaringBitmap>, k: usize) -> Result<Vec<Hit>> {
        let Query::Key { keys } = q else {
            return Err(Error::InvalidQuery(
                "the key index takes key queries".into(),
            ));
        };
        let mut out = Vec::new();
        for key in keys.iter() {
            if let KeyRows::Rows(r) = self.lookup(key) {
                for o in r.into_iter().flatten() {
                    if allow.is_none_or(|a| u32::try_from(o).is_ok_and(|o| a.contains(o))) {
                        out.push(Hit {
                            offset: o,
                            score: 0.0,
                            exact: false,
                        });
                    }
                }
            }
        }
        out.truncate(k);
        Ok(out)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Canonical bytes of a key given as scalars, cast to the key columns' types.
pub(crate) fn key_bytes(
    schema: &Schema,
    columns: &[String],
    key: &[datafusion::common::ScalarValue],
) -> Result<Vec<u8>> {
    let mut arrays = Vec::with_capacity(columns.len());
    for (c, v) in columns.iter().zip(key) {
        let f = schema
            .field_with_name(c)
            .map_err(|_| crate::schema::column_not_found(c, schema))?;
        let a = v.to_array()?;
        let a = if a.data_type() == f.data_type() {
            a
        } else {
            arrow::compute::cast(&a, f.data_type())?
        };
        arrays.push(a);
    }
    encode_row(&arrays, 0)
}

impl crate::table::TableInner {
    /// Where `key` (canonical bytes) may be in `f`, from its key index when this handle already
    /// knows one (from a listing or its own write). `None`: no key index known or usable; the
    /// caller reads the file with bloom filters and page statistics, still exactly.
    pub(crate) async fn key_rows(
        &self,
        def: &TableDef,
        f: &crate::log::DataFile,
        key: &[u8],
    ) -> Option<KeyRows> {
        let rt = &self.db.index;
        if matches!(rt.cache.mode(), super::cache::CacheMode::Disabled) {
            return None;
        }
        let spec = key_spec(def)?;
        let hash = super::spec::spec_hash(&KeyKind, &spec);
        let state = rt.table(self.idx_prefix().as_ref());
        let now = self.db.clock().now();
        state
            .discovery
            .refresh_if_stale(
                self.db.store.object_store(),
                &self.idx_prefix(),
                now,
                rt.cfg.poll,
            )
            .await;
        let listed = state
            .discovery
            .known(&f.path, KEY_INDEX)
            .into_iter()
            .find(|l| l.spec_hash == hash && !state.discovery.is_bad(&l.location, now))?;
        let index =
            super::query::open_listed(self, &KeyKind, &spec, &hash, f, &listed, &state).await?;
        let k = index.as_any().downcast_ref::<KeyIndex>()?;
        Some(k.lookup(key))
    }
}
