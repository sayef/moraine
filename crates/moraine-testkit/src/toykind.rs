//! A toy index kind defined outside moraine (D29: kinds are pluggable from outside the crate).
//!
//! `test-sum` indexes one `Int64` column: its sidecar holds `(offset, value)` pairs sorted by
//! value, and a query returns the rows with the largest values (`Order::Descending`). It uses
//! nothing but the public traits, so it proves the bundle, cache, discovery, builder and query
//! code need no change to serve a new kind.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Schema};
use moraine::index::bundle::{BundleView, SectionSink};
use moraine::index::{
    BuildCtx, BuildSummary, ExactScorer, FileIndex, Hit, IndexBuilder, IndexKind, IndexSpec, Order,
    Query,
};
use moraine::{Error, Result};
use roaring::RoaringBitmap;
use serde_json::json;

/// See the module documentation.
#[derive(Debug, Default)]
pub struct SumKind;

fn column(batch: &RecordBatch) -> Result<&Int64Array> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::InvalidQuery("test-sum indexes an Int64 column".into()))
}

impl IndexKind for SumKind {
    fn name(&self) -> &'static str {
        "test-sum"
    }

    fn format_version(&self) -> u32 {
        1
    }

    fn library_fingerprint(&self) -> String {
        "toy-1".into()
    }

    fn order(&self) -> Order {
        Order::Descending
    }

    fn resolve(&self, spec: &IndexSpec, schema: &Schema) -> Result<IndexSpec> {
        let [c] = spec.columns.as_slice() else {
            return Err(Error::InvalidIndexSpec("test-sum takes one column".into()));
        };
        let f = schema
            .field_with_name(c)
            .map_err(|_| Error::InvalidIndexSpec(format!("no column {c}")))?;
        if f.data_type() != &DataType::Int64 {
            return Err(Error::InvalidIndexSpec(format!("{c} is not Int64")));
        }
        let mut s = spec.clone();
        if s.params.is_null() {
            s.params = json!({});
        }
        if s.policy.is_null() {
            s.policy = json!({"min_rows": 1});
        }
        Ok(s)
    }

    fn wants_sidecar(&self, spec: &IndexSpec, file_rows: u64) -> bool {
        let min = spec
            .policy
            .get("min_rows")
            .and_then(|v| v.as_u64())
            .unwrap_or(1);
        file_rows >= min
    }

    fn build_bytes_per_row(&self, _spec: &IndexSpec) -> usize {
        16
    }

    fn builder(&self, _spec: &IndexSpec, _ctx: &BuildCtx) -> Result<Box<dyn IndexBuilder>> {
        Ok(Box::new(SumBuilder::default()))
    }

    fn open(&self, _spec: &IndexSpec, bundle: BundleView) -> Result<Arc<dyn FileIndex>> {
        let raw = bundle.section("pairs").unwrap_or_default();
        let (chunks, rest) = raw.as_chunks::<16>();
        if !rest.is_empty() {
            return Err(Error::Internal("test-sum: bad pairs section".into()));
        }
        let pairs = chunks
            .iter()
            .map(|c| {
                let o = u64::from_le_bytes(c[0..8].try_into().unwrap_or_default());
                let v = i64::from_le_bytes(c[8..16].try_into().unwrap_or_default());
                (o, v)
            })
            .collect();
        Ok(Arc::new(SumIndex { pairs }))
    }

    fn exact(&self, _spec: &IndexSpec, _q: &Query<'_>) -> Result<Box<dyn ExactScorer>> {
        Ok(Box::new(SumScorer))
    }
}

#[derive(Default)]
struct SumBuilder {
    pairs: Vec<(u64, i64)>,
}

impl IndexBuilder for SumBuilder {
    fn add(&mut self, first_row: u64, batch: &RecordBatch) -> Result<()> {
        let c = column(batch)?;
        for i in 0..c.len() {
            if c.is_valid(i) {
                self.pairs.push((first_row + i as u64, c.value(i)));
            }
        }
        Ok(())
    }

    fn memory_bytes(&self) -> usize {
        self.pairs.capacity() * 16
    }

    fn seal_part(&mut self, _sink: &mut dyn SectionSink) -> Result<()> {
        Ok(())
    }

    fn finish(mut self: Box<Self>, sink: &mut dyn SectionSink) -> Result<BuildSummary> {
        self.pairs.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        let w = sink.section("pairs")?;
        for (o, v) in &self.pairs {
            w.write_all(&o.to_le_bytes())?;
            w.write_all(&v.to_le_bytes())?;
        }
        Ok(BuildSummary {
            indexed_rows: self.pairs.len() as u64,
            parts: 1,
        })
    }
}

struct SumIndex {
    pairs: Vec<(u64, i64)>,
}

impl FileIndex for SumIndex {
    fn resident_bytes(&self) -> usize {
        self.pairs.len() * 16
    }

    fn search(&self, _q: &Query<'_>, allow: Option<&RoaringBitmap>, k: usize) -> Result<Vec<Hit>> {
        Ok(self
            .pairs
            .iter()
            .filter(|(o, _)| allow.is_none_or(|a| u32::try_from(*o).is_ok_and(|o| a.contains(o))))
            .take(k)
            .map(|(o, v)| Hit {
                offset: *o,
                score: *v as f32,
                exact: true,
            })
            .collect())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct SumScorer;

impl ExactScorer for SumScorer {
    fn score(&self, batch: &RecordBatch) -> Result<Vec<Option<f32>>> {
        let c = column(batch)?;
        Ok((0..c.len())
            .map(|i| c.is_valid(i).then(|| c.value(i) as f32))
            .collect())
    }
}
