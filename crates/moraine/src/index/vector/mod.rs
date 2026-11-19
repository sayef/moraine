//! What the vector kinds share (D24): the column and metric of
//! a spec, the distance definitions, which vectors are indexable, query validation, and the
//! exact scorer that is both the no-sidecar route and the rescore of approximate hits.
//!
//! A vector column is `List<Float32>` (Delta has no fixed-size list; D33), so the spec names its
//! dimension. A row is indexed only when its vector is non-null, has that dimension, is finite,
//! and (under cosine) is not zero; every route skips the same rows, so they never compete.

// Without a vector kind, only `Metric` and the helpers full-text search shares are used.
#![cfg_attr(not(any(feature = "vector", feature = "turbovec")), allow(dead_code))]

#[cfg(feature = "turbovec")]
pub(crate) mod turbo;
#[cfg(feature = "vector")]
pub(crate) mod usearch;

#[cfg(feature = "turbovec")]
#[cfg_attr(docsrs, doc(cfg(feature = "turbovec")))]
pub use turbo::TurboKind;
#[cfg(feature = "vector")]
#[cfg_attr(docsrs, doc(cfg(feature = "vector")))]
pub use usearch::HnswKind;

use arrow::array::{
    Array, FixedSizeListArray, Float32Array, LargeListArray, ListArray, RecordBatch,
};
use arrow::datatypes::{DataType, Schema};
use serde_json::Value;

use super::kind::{ExactScorer, Query};
use super::spec::IndexSpec;
use crate::error::{Error, Result};

/// How vectors are compared. Every kind returns distances (smaller is closer) with these
/// definitions, which are usearch's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Metric {
    /// `1 - cos(a, b)`.
    Cosine,
    /// Squared Euclidean distance.
    L2,
    /// `1 - a·b`.
    InnerProduct,
}

impl Metric {
    /// Parses a spec's `metric`: `cosine` (or `cos`), `l2` (or `l2sq`), `ip` (or
    /// `inner_product`, `dot`).
    pub fn parse(s: &str) -> Result<Metric> {
        match s {
            "cosine" | "cos" => Ok(Metric::Cosine),
            "l2" | "l2sq" => Ok(Metric::L2),
            "ip" | "inner_product" | "dot" => Ok(Metric::InnerProduct),
            other => Err(Error::InvalidIndexSpec(format!(
                "unknown metric {other:?} (cosine, l2, ip)"
            ))),
        }
    }

    /// The canonical name, as resolved specs store it.
    pub fn name(self) -> &'static str {
        match self {
            Metric::Cosine => "cosine",
            Metric::L2 => "l2",
            Metric::InnerProduct => "ip",
        }
    }

    /// The distance between `q` and `x` (same length).
    pub fn distance(self, q: &[f32], x: &[f32]) -> f32 {
        match self {
            Metric::L2 => q
                .iter()
                .zip(x)
                .map(|(a, b)| {
                    let d = a - b;
                    d * d
                })
                .sum(),
            Metric::InnerProduct => 1.0 - dot(q, x),
            Metric::Cosine => {
                let (ab, aa, bb) = q
                    .iter()
                    .zip(x)
                    .fold((0f32, 0f32, 0f32), |(ab, aa, bb), (a, b)| {
                        (ab + a * b, aa + a * a, bb + b * b)
                    });
                1.0 - ab / (aa.sqrt() * bb.sqrt())
            }
        }
    }
}

/// The dot product of `a` and `b` (over the shorter length).
pub(crate) fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// The parameters every vector spec has.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VectorParams {
    /// The vector column.
    pub column: String,
    /// The dimension every vector must have (D33).
    pub dim: usize,
    /// The distance.
    pub metric: Metric,
}

impl VectorParams {
    /// Reads them from a resolved spec.
    pub fn of(spec: &IndexSpec) -> Result<VectorParams> {
        let bad = |m: &str| Error::InvalidIndexSpec(format!("index {}: {m}", spec.name));
        let [column] = spec.columns.as_slice() else {
            return Err(bad("a vector index has exactly one column"));
        };
        let dim = spec
            .params
            .get("dim")
            .and_then(Value::as_u64)
            .filter(|d| (1..=65_536).contains(d))
            .ok_or_else(|| bad("params.dim must be between 1 and 65,536"))?
            as usize;
        let metric = Metric::parse(
            spec.params
                .get("metric")
                .and_then(Value::as_str)
                .ok_or_else(|| bad("params.metric is missing"))?,
        )?;
        Ok(VectorParams {
            column: column.clone(),
            dim,
            metric,
        })
    }

    /// Checks the column against the schema; fills `dim` from a fixed-size list and `metric`
    /// with cosine. Returns the params object with both set.
    pub fn resolve(spec: &IndexSpec, schema: &Schema) -> Result<serde_json::Map<String, Value>> {
        let bad = |m: String| Error::InvalidIndexSpec(format!("index {}: {m}", spec.name));
        let [column] = spec.columns.as_slice() else {
            return Err(bad("a vector index has exactly one column".into()));
        };
        let f = schema
            .field_with_name(column)
            .map_err(|_| bad(format!("no column {column}")))?;
        let fixed = match f.data_type() {
            DataType::List(i) | DataType::LargeList(i) if i.data_type() == &DataType::Float32 => {
                None
            }
            DataType::FixedSizeList(i, n) if i.data_type() == &DataType::Float32 => Some(*n as u64),
            other => {
                return Err(bad(format!(
                    "column {column} has type {other}; a vector column is a list of Float32"
                )));
            }
        };
        let mut p = match &spec.params {
            Value::Object(m) => m.clone(),
            Value::Null => serde_json::Map::new(),
            _ => return Err(bad("params must be an object".into())),
        };
        if let Some(n) = fixed {
            match p.get("dim").and_then(Value::as_u64) {
                Some(d) if d != n => return Err(bad(format!("dim {d}, the column has {n}"))),
                _ => {
                    p.insert("dim".into(), n.into());
                }
            }
        }
        p.entry("metric").or_insert_with(|| "cosine".into());
        let probe = IndexSpec {
            params: Value::Object(p.clone()),
            ..spec.clone()
        };
        let vp = VectorParams::of(&probe)?;
        p.insert("metric".into(), vp.metric.name().into());
        Ok(p)
    }

    /// Whether `v` is indexed and can be returned (see the module documentation).
    pub fn usable(&self, v: &[f32]) -> bool {
        v.len() == self.dim
            && v.iter().all(|x| x.is_finite())
            && (self.metric != Metric::Cosine || v.iter().any(|x| *x != 0.0))
    }

    /// The query vector checked (case V11).
    pub fn check_query(&self, q: &Query<'_>) -> Result<Vec<f32>> {
        let Query::Vector { vector, .. } = q else {
            return Err(Error::InvalidQuery(
                "a vector index takes a vector query".into(),
            ));
        };
        if vector.len() != self.dim {
            return Err(Error::InvalidQuery(format!(
                "the query has {} dimensions, the index {}",
                vector.len(),
                self.dim
            )));
        }
        if vector.iter().any(|x| !x.is_finite()) {
            return Err(Error::InvalidQuery("the query vector is not finite".into()));
        }
        if self.metric == Metric::Cosine && vector.iter().all(|x| *x == 0.0) {
            return Err(Error::InvalidQuery(
                "a zero query vector has no direction under the cosine metric".into(),
            ));
        }
        Ok(vector.to_vec())
    }
}

/// The vectors of a column, one per row: `None` for nulls.
pub(crate) struct Vectors<'a> {
    values: &'a [f32],
    offsets: Offsets<'a>,
    nulls: Option<&'a arrow::buffer::NullBuffer>,
    len: usize,
}

enum Offsets<'a> {
    I32(&'a [i32]),
    I64(&'a [i64]),
    Fixed(usize),
}

impl<'a> Vectors<'a> {
    /// The vectors of `a`: a list, large list or fixed-size list of non-null Float32. Null items
    /// inside a vector make that vector unusable (it reads as `None`).
    pub fn of(a: &'a dyn Array) -> Result<Vectors<'a>> {
        let bad = || Error::InvalidQuery(format!("not a vector column: {}", a.data_type()));
        let (values, offsets): (&dyn Array, Offsets<'a>) =
            if let Some(l) = a.as_any().downcast_ref::<ListArray>() {
                (l.values().as_ref(), Offsets::I32(l.value_offsets()))
            } else if let Some(l) = a.as_any().downcast_ref::<LargeListArray>() {
                (l.values().as_ref(), Offsets::I64(l.value_offsets()))
            } else if let Some(l) = a.as_any().downcast_ref::<FixedSizeListArray>() {
                (
                    l.values().as_ref(),
                    Offsets::Fixed(l.value_length() as usize),
                )
            } else {
                return Err(bad());
            };
        let f = values
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(bad)?;
        // A vector with a null item is unusable; such columns are rare, so all rows of one
        // read as null rather than checking every item.
        let values: &'a [f32] = if f.null_count() > 0 { &[] } else { f.values() };
        Ok(Vectors {
            values,
            offsets,
            nulls: a.nulls(),
            len: a.len(),
        })
    }

    /// Rows in the column.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Row `i`'s vector, `None` when null (or when the column holds null items).
    pub fn get(&self, i: usize) -> Option<&'a [f32]> {
        if self.nulls.is_some_and(|n| n.is_null(i)) {
            return None;
        }
        let (s, e) = match self.offsets {
            Offsets::I32(o) => (o[i] as usize, o[i + 1] as usize),
            Offsets::I64(o) => (o[i] as usize, o[i + 1] as usize),
            Offsets::Fixed(n) => (i * n, (i + 1) * n),
        };
        self.values.get(s..e)
    }
}

/// Exact distances for a query (the no-sidecar route, and rescoring).
pub(crate) struct ExactVector {
    /// The index's vector parameters.
    pub params: VectorParams,
    /// The query vector.
    pub query: Vec<f32>,
}

impl ExactScorer for ExactVector {
    fn score(&self, batch: &RecordBatch) -> Result<Vec<Option<f32>>> {
        let v = Vectors::of(batch.column(0).as_ref())?;
        Ok((0..v.len())
            .map(|i| {
                v.get(i)
                    .filter(|x| self.params.usable(x))
                    .map(|x| self.params.metric.distance(&self.query, x))
            })
            .collect())
    }
}

/// `base` (an object or null) with `defaults` filled in where absent.
pub(crate) fn with_defaults(base: &Value, defaults: Value) -> Value {
    let mut m = match base {
        Value::Object(m) => m.clone(),
        _ => serde_json::Map::new(),
    };
    if let Value::Object(d) = defaults {
        for (k, v) in d {
            m.entry(k).or_insert(v);
        }
    }
    Value::Object(m)
}

/// Reads an unsigned integer parameter of a spec section, with a default.
pub(crate) fn uint(v: &Value, key: &str, default: u64) -> u64 {
    v.get(key).and_then(Value::as_u64).unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Builder, ListBuilder};
    use std::sync::Arc;

    #[test]
    fn distances() {
        let a = [1.0f32, 0.0];
        let b = [0.0f32, 2.0];
        assert_eq!(Metric::L2.distance(&a, &b), 5.0);
        assert_eq!(Metric::InnerProduct.distance(&a, &b), 1.0);
        assert!((Metric::Cosine.distance(&a, &b) - 1.0).abs() < 1e-7);
        assert!(Metric::Cosine.distance(&a, &[3.0, 0.0]).abs() < 1e-7);
    }

    #[test]
    fn vectors_of_lists() {
        let mut b = ListBuilder::new(Float32Builder::new());
        b.values().append_slice(&[1.0, 2.0]);
        b.append(true);
        b.append(false);
        b.values().append_slice(&[3.0]);
        b.append(true);
        let a = Arc::new(b.finish());
        let v = Vectors::of(a.as_ref()).unwrap();
        assert_eq!(v.get(0), Some(&[1.0f32, 2.0][..]));
        assert_eq!(v.get(1), None);
        assert_eq!(v.get(2), Some(&[3.0f32][..]));
        let p = VectorParams {
            column: "v".into(),
            dim: 2,
            metric: Metric::Cosine,
        };
        assert!(p.usable(&[1.0, 2.0]));
        assert!(!p.usable(&[3.0]));
        assert!(!p.usable(&[0.0, 0.0]));
        assert!(!p.usable(&[f32::NAN, 1.0]));
    }
}
