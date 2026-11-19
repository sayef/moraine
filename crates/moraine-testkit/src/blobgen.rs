//! Rows with a blob column for the blob tests: a key, the name of the value's source, and the
//! value, whose bytes follow from the source name and the length.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use moraine::blob::{self, BlobColumn, BlobRef};

/// `id`, `src` and the blob column `body`.
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("src", DataType::Utf8, false),
        blob::field("body", true),
    ]))
}

/// The bytes of value `src` of length `n`.
pub fn content(src: &str, n: usize) -> Bytes {
    let mut x = src.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3)
    }) | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect::<Vec<u8>>()
        .into()
}

/// A value given as bytes or as a reference read from another row.
pub enum Body {
    Bytes(Bytes),
    Ref(BlobRef),
}

/// One row `id` whose value comes from `src`.
pub fn row(id: &str, src: &str, body: &Body) -> RecordBatch {
    let mut c = BlobColumn::new();
    match body {
        Body::Bytes(b) => c.push_bytes(b, None),
        Body::Ref(r) => c.push_ref(r),
    }
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(vec![id])) as ArrayRef,
            Arc::new(StringArray::from(vec![src])),
            c.finish(),
        ],
    )
    .expect("batch")
}
