//! Data generators: row batches with controllable key overlap and hostile
//! strings for keys and names. Vectors, text and blobs have generators of their own.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::rng::SimRng;

/// Strings that break naive code: empty, spaces, separators, escapes, percent signs, Unicode
/// that only normalises equal (case D8), right-to-left text, very long values.
pub const HOSTILE: &[&str] = &[
    "",
    " ",
    "a b",
    "a/b",
    "../x",
    "a%20b",
    "%",
    "\\",
    "\"quoted\"",
    "line\nbreak",
    "tab\t",
    "\u{e9}",
    "e\u{301}",
    "\u{5e9}\u{5dc}\u{5d5}\u{5dd}",
    "\u{1F600}",
    "\u{10FFFF}",
    "NUL\u{0}",
    "z",
];

/// A string from [`HOSTILE`], or a random lowercase one, or a long one.
pub fn hostile_string(rng: &mut SimRng) -> String {
    match rng.below(10) {
        0..=5 => HOSTILE[rng.below(HOSTILE.len() as u64) as usize].to_string(),
        6 => "x".repeat(300 + rng.below(100) as usize),
        _ => (0..rng.range(1, 12))
            .map(|_| (b'a' + rng.below(26) as u8) as char)
            .collect(),
    }
}

/// The schema of [`rows`]: key `k`, operation id `op`, row number `i`, group `g`.
pub fn row_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("op", DataType::Utf8, false),
        Field::new("i", DataType::Int64, false),
        Field::new("g", DataType::Int64, true),
    ]))
}

/// `n` rows of operation `op`. Keys are drawn from `key_space` distinct values (prefixed with a
/// hostile string), so a small key space makes operations overlap; `None` makes every key
/// unique to the operation. Groups `g` are drawn from `groups` values, some null.
pub fn rows(
    rng: &mut SimRng,
    op: &str,
    n: usize,
    key_space: Option<u64>,
    groups: u64,
) -> RecordBatch {
    let mut k = Vec::with_capacity(n);
    let mut g = Vec::with_capacity(n);
    for i in 0..n {
        let tag = hostile_string(rng);
        k.push(match key_space {
            Some(space) => format!("{tag}#{}", rng.below(space.max(1))),
            None => format!("{tag}#{op}#{i}"),
        });
        g.push((!rng.chance(0.1)).then(|| rng.below(groups.max(1)) as i64));
    }
    RecordBatch::try_new(
        row_schema(),
        vec![
            Arc::new(StringArray::from(k)),
            Arc::new(StringArray::from(vec![op; n])),
            Arc::new(Int64Array::from_iter_values(0..n as i64)),
            Arc::new(Int64Array::from(g)),
        ],
    )
    .expect("generated batch")
}
