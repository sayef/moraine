//! Vector data for index tests (a data generator): clustered and uniform
//! vectors from a seed, as row batches with an id, a group and a `List<Float32>` column.

use std::sync::Arc;

use arrow::array::{Float32Builder, Int64Array, ListBuilder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::rng::SimRng;

/// The table schema of [`batch`]: `id` (key), `grp` and `v` (vectors; Delta's list item name).
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("grp", DataType::Int64, true),
        Field::new("v", list_type(), true),
    ]))
}

/// `List<Float32>` as Delta stores it.
pub fn list_type() -> DataType {
    DataType::List(Arc::new(Field::new("element", DataType::Float32, true)))
}

/// A standard normal sample (Box-Muller).
pub fn normal(rng: &mut SimRng) -> f32 {
    let u1 = ((rng.next_u64() >> 11) as f64 + 1.0) / (1u64 << 53) as f64;
    let u2 = (rng.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
    ((-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()) as f32
}

/// Centres of clustered data, fixed by `seed`.
pub fn centres(seed: u64, clusters: usize, dim: usize) -> Vec<Vec<f32>> {
    let mut rng = SimRng::new(seed);
    (0..clusters)
        .map(|_| (0..dim).map(|_| normal(&mut rng)).collect())
        .collect()
}

/// `n` vectors around `centres` with Gaussian spread `spread` (relative to unit centres).
pub fn clustered(rng: &mut SimRng, centres: &[Vec<f32>], n: usize, spread: f32) -> Vec<Vec<f32>> {
    (0..n)
        .map(|_| {
            let c = &centres[rng.below(centres.len() as u64) as usize];
            c.iter().map(|x| x + spread * normal(rng)).collect()
        })
        .collect()
}

/// Rows `first_id..` with the given vectors (`None` is a null vector) and `grp = id % 10`.
pub fn batch(first_id: i64, vectors: &[Option<Vec<f32>>]) -> RecordBatch {
    let ids: Vec<i64> = (0..vectors.len() as i64).map(|i| first_id + i).collect();
    let grp: Vec<i64> = ids.iter().map(|i| i % 10).collect();
    let mut b = ListBuilder::new(Float32Builder::new()).with_field(Arc::new(Field::new(
        "element",
        DataType::Float32,
        true,
    )));
    for v in vectors {
        match v {
            Some(v) => {
                b.values().append_slice(v);
                b.append(true);
            }
            None => b.append(false),
        }
    }
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(grp)),
            Arc::new(b.finish()),
        ],
    )
    .expect("vector batch")
}
