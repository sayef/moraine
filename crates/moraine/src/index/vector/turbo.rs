//! The `vec-tq` kind: TurboQuant codes per data file through the `turbovec` crate (D24).
//!
//! Each vector is rotated with a fixed seed and quantised to 2 to 4 bits per dimension, with
//! no training, so equal rows give byte-identical sidecars and two files' codes concatenate
//! into the merged file's sidecar without reading floats. Scores are inner products (turbovec
//! has no other metric, D34): under cosine the vectors and queries are normalised first, and
//! the distance is `1 - score` either way. Scores are estimates, so hits are rescored from the
//! rows (`rerank`, default 8 extra candidates per hit; D37).
//!
//! A part is one section `tq.<n>`: `u32` count, `u32` dimension, `u8` bits, three zero bytes,
//! then the row offsets (`u32` each), the scales (`f32` each) and the packed codes
//! (`dim * bits / 8` bytes each). At 256 dimensions and 4 bits that is 136 bytes per vector.
//! Opening copies the codes into a turbovec index; its search layout is prepared on the first
//! search, so a handle's resident cost is about twice the code bytes.

use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use roaring::RoaringBitmap;
use serde_json::{Value, json};
use turbovec::TurboQuantIndex;

use super::{ExactVector, Metric, VectorParams, Vectors, uint, with_defaults as merge};
use crate::error::{Error, Result};
use crate::index::bundle::{BundleView, SectionSink};
use crate::index::kind::{
    BuildCtx, BuildSummary, ExactScorer, FileIndex, Hit, IndexBuilder, IndexKind, MergeInput,
    Order, Query,
};
use crate::index::spec::IndexSpec;
use crate::index::topk::{Candidate, TopK};

pub const DEFAULT_BITS: u64 = 4;
pub const DEFAULT_RERANK: u64 = 8;
pub const DEFAULT_MIN_ROWS_FOR_ANN: u64 = 10_000;
const HEAD: usize = 12;

/// See the module documentation.
#[derive(Debug, Default)]
pub struct TurboKind;

struct Params {
    v: VectorParams,
    bits: usize,
}

impl Params {
    fn of(spec: &IndexSpec) -> Result<Params> {
        let v = VectorParams::of(spec)?;
        let bits = uint(&spec.params, "bits", DEFAULT_BITS) as usize;
        let bad = |m: String| Error::InvalidIndexSpec(format!("index {}: {m}", spec.name));
        if !(2..=4).contains(&bits) {
            return Err(bad(format!("bits must be 2, 3 or 4, not {bits}")));
        }
        if !v.dim.is_multiple_of(8) || v.dim > turbovec::MAX_DIM {
            return Err(bad(format!(
                "vec-tq needs a dimension that is a multiple of 8 up to {}, not {}",
                turbovec::MAX_DIM,
                v.dim
            )));
        }
        if v.metric == Metric::L2 {
            return Err(bad(
                "vec-tq scores inner products; use the cosine or ip metric".into(),
            ));
        }
        Ok(Params { v, bits })
    }

    fn stride(&self) -> usize {
        self.v.dim / 8 * self.bits
    }

    /// The vector turbovec sees: normalised under cosine.
    fn prepare(&self, x: &[f32], out: &mut Vec<f32>) {
        match self.v.metric {
            Metric::Cosine => {
                let n = super::dot(x, x).sqrt();
                out.extend(x.iter().map(|v| v / n));
            }
            _ => out.extend_from_slice(x),
        }
    }
}

fn tq(e: impl std::fmt::Display) -> Error {
    Error::Internal(format!("turbovec: {e}"))
}

/// A fixed probe encoded at load time: a change of rotation seed or quantiser changes the
/// codes, hence the fingerprint, hence every spec hash.
fn probe() -> &'static str {
    static P: OnceLock<String> = OnceLock::new();
    P.get_or_init(|| {
        let dim = 64;
        let flat: Vec<f32> = (0..8 * dim).map(|i| ((i as f32) * 0.37).sin()).collect();
        let mut out = String::from("turbovec-1");
        for bits in [2usize, 4] {
            let h = match TurboQuantIndex::new(dim, bits) {
                Ok(mut idx) => {
                    idx.add(&flat);
                    let mut b = idx.packed_codes().to_vec();
                    for s in idx.scales() {
                        b.extend_from_slice(&s.to_le_bytes());
                    }
                    xxhash_rust::xxh3::xxh3_64(&b)
                }
                Err(_) => 0,
            };
            out.push_str(&format!("-{h:016x}"));
        }
        out
    })
}

impl IndexKind for TurboKind {
    fn name(&self) -> &'static str {
        "vec-tq"
    }

    fn format_version(&self) -> u32 {
        1
    }

    fn library_fingerprint(&self) -> String {
        probe().to_string()
    }

    fn order(&self) -> Order {
        Order::Ascending
    }

    fn resolve(&self, spec: &IndexSpec, schema: &Schema) -> Result<IndexSpec> {
        let mut p = VectorParams::resolve(spec, schema)?;
        p.entry("bits").or_insert_with(|| DEFAULT_BITS.into());
        let mut s = spec.clone();
        s.params = Value::Object(p);
        Params::of(&s)?;
        s.search = merge(&s.search, json!({"rerank": DEFAULT_RERANK}));
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
            // Codes, scale and offset, doubled for vectors growing by doubling, and doubled
            // again for turbovec's working copies while encoding (measured against the peak
            // resident memory of a 300,000-row build).
            Ok(p) => (p.stride() + 8) * 4 + 16,
            Err(_) => 1024,
        }
    }

    fn builder(&self, spec: &IndexSpec, _ctx: &BuildCtx) -> Result<Box<dyn IndexBuilder>> {
        let p = Params::of(spec)?;
        Ok(Box::new(TqBuilder {
            index: None,
            offsets: Vec::new(),
            parts: 0,
            indexed: 0,
            flat: Vec::new(),
            merged: None,
            p,
        }))
    }

    /// Concatenates the inputs' codes in the merged file's row order: no floats are read and
    /// nothing is quantised again (D24).
    fn merge(
        &self,
        spec: &IndexSpec,
        inputs: &[MergeInput<'_>],
        _ctx: &BuildCtx,
    ) -> Result<Option<Box<dyn IndexBuilder>>> {
        let p = Params::of(spec)?;
        let stride = p.stride();
        let mut rows: Vec<(u64, f32, Vec<u8>)> = Vec::new();
        for input in inputs {
            for (_, bytes) in input.bundle.sections_with_prefix("tq.") {
                let part = Part::parse(bytes, &p)?;
                for i in 0..part.offsets.len() {
                    let old = u64::from(part.offsets[i]);
                    if let Some(Some(new)) = input.rows.new.get(old as usize) {
                        rows.push((
                            *new,
                            part.scales[i],
                            part.codes[i * stride..(i + 1) * stride].to_vec(),
                        ));
                    }
                }
            }
        }
        rows.sort_by_key(|r| r.0);
        let mut m = Merged::default();
        for (o, s, c) in rows {
            m.offsets
                .push(u32::try_from(o).map_err(|_| tq("row offset beyond u32"))?);
            m.scales.push(s);
            m.codes.extend_from_slice(&c);
        }
        Ok(Some(Box::new(TqBuilder {
            index: None,
            offsets: Vec::new(),
            parts: 0,
            indexed: m.offsets.len() as u64,
            flat: Vec::new(),
            merged: Some(m),
            p,
        })))
    }

    fn open(&self, spec: &IndexSpec, bundle: BundleView) -> Result<Arc<dyn FileIndex>> {
        let p = Params::of(spec)?;
        let mut parts = Vec::new();
        for (_, bytes) in bundle.sections_with_prefix("tq.") {
            let part = Part::parse(bytes, &p)?;
            let n = part.offsets.len();
            let index = TurboQuantIndex::from_parts(
                Some(p.v.dim),
                p.bits,
                n,
                part.codes.to_vec(),
                part.scales.clone(),
                Vec::new(),
                Vec::new(),
            )
            .map_err(tq)?;
            parts.push(OpenPart {
                offsets: part.offsets,
                index,
            });
        }
        Ok(Arc::new(TqIndex {
            metric: p.v.metric,
            parts,
        }))
    }

    fn exact(&self, spec: &IndexSpec, q: &Query<'_>) -> Result<Box<dyn ExactScorer>> {
        let params = VectorParams::of(spec)?;
        let query = params.check_query(q)?;
        Ok(Box::new(ExactVector { params, query }))
    }

    fn rerank(&self, _spec: &IndexSpec, q: &Query<'_>) -> usize {
        uint(q.params(), "rerank", DEFAULT_RERANK) as usize
    }

    fn validate(&self, spec: &IndexSpec, q: &Query<'_>) -> Result<()> {
        Params::of(spec)?.v.check_query(q).map(drop)
    }
}

/// A part's section, parsed and checked.
struct Part<'a> {
    offsets: Vec<u32>,
    scales: Vec<f32>,
    codes: &'a [u8],
}

impl<'a> Part<'a> {
    fn parse(b: &'a [u8], p: &Params) -> Result<Part<'a>> {
        let bad = |m: &str| Error::Internal(format!("vec-tq: corrupt section ({m})"));
        if b.len() < HEAD {
            return Err(bad("short"));
        }
        let n = u32::from_le_bytes(b[0..4].try_into().unwrap_or_default()) as usize;
        let dim = u32::from_le_bytes(b[4..8].try_into().unwrap_or_default()) as usize;
        if dim != p.v.dim || b[8] as usize != p.bits || b[9..12] != [0, 0, 0] {
            return Err(bad("dimension or bits"));
        }
        let stride = p.stride();
        let need = n
            .checked_mul(8 + stride)
            .and_then(|x| x.checked_add(HEAD))
            .ok_or_else(|| bad("size"))?;
        if b.len() != need {
            return Err(bad("size"));
        }
        let o_end = HEAD + 4 * n;
        let s_end = o_end + 4 * n;
        let offsets: Vec<u32> = b[HEAD..o_end]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|c| u32::from_le_bytes(*c))
            .collect();
        let scales: Vec<f32> = b[o_end..s_end]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|c| f32::from_le_bytes(*c))
            .collect();
        if offsets.windows(2).any(|w| w[0] >= w[1]) || scales.iter().any(|s| !s.is_finite()) {
            return Err(bad("order or scales"));
        }
        Ok(Part {
            offsets,
            scales,
            codes: &b[s_end..],
        })
    }
}

fn write_part(
    sink: &mut dyn SectionSink,
    name: &str,
    p: &Params,
    offsets: &[u32],
    scales: &[f32],
    codes: &[u8],
) -> Result<()> {
    let w = sink.section(name)?;
    w.write_all(&(offsets.len() as u32).to_le_bytes())?;
    w.write_all(&(p.v.dim as u32).to_le_bytes())?;
    w.write_all(&[p.bits as u8, 0, 0, 0])?;
    let mut buf = Vec::with_capacity(offsets.len() * 8);
    for o in offsets {
        buf.extend_from_slice(&o.to_le_bytes());
    }
    for s in scales {
        buf.extend_from_slice(&s.to_le_bytes());
    }
    w.write_all(&buf)?;
    w.write_all(codes)?;
    Ok(())
}

#[derive(Default)]
struct Merged {
    offsets: Vec<u32>,
    scales: Vec<f32>,
    codes: Vec<u8>,
}

struct TqBuilder {
    p: Params,
    index: Option<TurboQuantIndex>,
    offsets: Vec<u32>,
    parts: u32,
    indexed: u64,
    /// Reused buffer of one batch's vectors.
    flat: Vec<f32>,
    merged: Option<Merged>,
}

impl IndexBuilder for TqBuilder {
    fn add(&mut self, first_row: u64, batch: &RecordBatch) -> Result<()> {
        let v = Vectors::of(batch.column(0).as_ref())?;
        self.flat.clear();
        for i in 0..v.len() {
            let Some(x) = v.get(i).filter(|x| self.p.v.usable(x)) else {
                continue;
            };
            let o = u32::try_from(first_row + i as u64).map_err(|_| tq("row offset beyond u32"))?;
            self.offsets.push(o);
            self.p.prepare(x, &mut self.flat);
        }
        if self.flat.is_empty() {
            return Ok(());
        }
        if self.index.is_none() {
            self.index = Some(TurboQuantIndex::new(self.p.v.dim, self.p.bits).map_err(tq)?);
        }
        if let Some(idx) = self.index.as_mut() {
            idx.add_2d(&self.flat, self.p.v.dim).map_err(tq)?;
        }
        self.indexed += (self.flat.len() / self.p.v.dim) as u64;
        Ok(())
    }

    fn memory_bytes(&self) -> usize {
        // turbovec's codes and scales grow like our offsets (by doubling): count capacity.
        let n = self.offsets.capacity();
        n * (self.p.stride() + 8) * 2 + self.flat.capacity() * 4
    }

    fn seal_part(&mut self, sink: &mut dyn SectionSink) -> Result<()> {
        let Some(idx) = self.index.take() else {
            return Ok(());
        };
        write_part(
            sink,
            &format!("tq.{}", self.parts),
            &self.p,
            &self.offsets,
            idx.scales(),
            idx.packed_codes(),
        )?;
        self.offsets.clear();
        self.parts += 1;
        Ok(())
    }

    fn finish(mut self: Box<Self>, sink: &mut dyn SectionSink) -> Result<BuildSummary> {
        if let Some(m) = self.merged.take() {
            if !m.offsets.is_empty() {
                write_part(sink, "tq.0", &self.p, &m.offsets, &m.scales, &m.codes)?;
                self.parts = 1;
            }
        } else {
            self.seal_part(sink)?;
        }
        Ok(BuildSummary {
            indexed_rows: self.indexed,
            parts: self.parts,
        })
    }
}

struct OpenPart {
    offsets: Vec<u32>,
    index: TurboQuantIndex,
}

struct TqIndex {
    metric: Metric,
    parts: Vec<OpenPart>,
}

impl FileIndex for TqIndex {
    fn resident_bytes(&self) -> usize {
        self.parts
            .iter()
            .map(|p| p.offsets.len() * 8 + p.index.packed_codes().len() * 2)
            .sum::<usize>()
            + 4096
    }

    fn search(&self, q: &Query<'_>, allow: Option<&RoaringBitmap>, k: usize) -> Result<Vec<Hit>> {
        let Query::Vector { vector, .. } = q else {
            return Err(Error::InvalidQuery("vec-tq takes a vector query".into()));
        };
        let mut query = Vec::with_capacity(vector.len());
        match self.metric {
            Metric::Cosine => {
                let n = super::dot(vector, vector).sqrt();
                query.extend(vector.iter().map(|v| v / n));
            }
            _ => query.extend_from_slice(vector),
        }
        let file: Arc<str> = "".into();
        let mut top = TopK::new(Order::Ascending, k);
        for p in &self.parts {
            let mask: Option<Vec<bool>> =
                allow.map(|a| p.offsets.iter().map(|o| a.contains(*o)).collect());
            let r = p
                .index
                .try_search_with_mask(&query, k, mask.as_deref())
                .map_err(tq)?;
            if r.nq == 0 {
                continue;
            }
            for (s, slot) in r.scores_for_query(0).iter().zip(r.indices_for_query(0)) {
                let Some(o) = usize::try_from(*slot).ok().and_then(|i| p.offsets.get(i)) else {
                    continue;
                };
                top.push(Candidate {
                    file: file.clone(),
                    offset: u64::from(*o),
                    score: 1.0 - s,
                    exact: false,
                });
            }
        }
        Ok(top
            .into_sorted()
            .into_iter()
            .map(|c| Hit {
                offset: c.offset,
                score: c.score,
                exact: false,
            })
            .collect())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
