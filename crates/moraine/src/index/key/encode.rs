//! Canonical key bytes (D19, D36): a stable, order-preserving encoding of key values, so key
//! sidecars do not depend on Arrow's row format (which promises no stability) and sorted keys
//! stay sorted as bytes.
//!
//! Per column: a tag byte, then the value. Nulls are the tag alone (0) and sort first.
//! Integers, dates and timestamps are 8 bytes big-endian with the sign bit flipped; floats are
//! their IEEE bits in total order; booleans one byte; decimals 16 bytes like integers; strings
//! and binary have each 0x00 escaped as 0x00 0xFF and end with 0x00 0x00. Comparing two
//! encodings byte by byte compares the keys column by column.

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, LargeBinaryArray, LargeStringArray,
    StringArray,
};
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};

use crate::error::{Error, Result};

const NULL: u8 = 0;
const INT: u8 = 1;
const FLOAT: u8 = 2;
const BOOL: u8 = 3;
const DECIMAL: u8 = 4;
const BYTES: u8 = 5;

fn int(out: &mut Vec<u8>, v: i64) {
    out.push(INT);
    out.extend_from_slice(&((v as u64) ^ (1 << 63)).to_be_bytes());
}

fn float(out: &mut Vec<u8>, v: f64) {
    let b = v.to_bits();
    let ordered = if b >> 63 == 1 { !b } else { b | (1 << 63) };
    out.push(FLOAT);
    out.extend_from_slice(&ordered.to_be_bytes());
}

fn bytes(out: &mut Vec<u8>, v: &[u8]) {
    out.push(BYTES);
    for b in v {
        out.push(*b);
        if *b == 0 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

/// Appends row `i` of `a` to `out`.
pub fn encode_value(a: &dyn Array, i: usize, out: &mut Vec<u8>) -> Result<()> {
    if a.is_null(i) {
        out.push(NULL);
        return Ok(());
    }
    match a.data_type() {
        DataType::Int8 => int(out, a.as_primitive::<Int8Type>().value(i).into()),
        DataType::Int16 => int(out, a.as_primitive::<Int16Type>().value(i).into()),
        DataType::Int32 => int(out, a.as_primitive::<Int32Type>().value(i).into()),
        DataType::Int64 => int(out, a.as_primitive::<Int64Type>().value(i)),
        DataType::Date32 => int(out, a.as_primitive::<Date32Type>().value(i).into()),
        DataType::Timestamp(u, _) => int(
            out,
            match u {
                TimeUnit::Second => a.as_primitive::<TimestampSecondType>().value(i),
                TimeUnit::Millisecond => a.as_primitive::<TimestampMillisecondType>().value(i),
                TimeUnit::Microsecond => a.as_primitive::<TimestampMicrosecondType>().value(i),
                TimeUnit::Nanosecond => a.as_primitive::<TimestampNanosecondType>().value(i),
            },
        ),
        DataType::Float32 => float(out, a.as_primitive::<Float32Type>().value(i).into()),
        DataType::Float64 => float(out, a.as_primitive::<Float64Type>().value(i)),
        DataType::Boolean => {
            let b = a
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| Error::Internal("boolean".into()))?;
            out.push(BOOL);
            out.push(u8::from(b.value(i)));
        }
        DataType::Decimal128(_, _) => {
            let v = a.as_primitive::<Decimal128Type>().value(i);
            out.push(DECIMAL);
            out.extend_from_slice(&((v as u128) ^ (1 << 127)).to_be_bytes());
        }
        DataType::Utf8 => bytes(
            out,
            a.as_any()
                .downcast_ref::<StringArray>()
                .map(|s| s.value(i).as_bytes())
                .unwrap_or_default(),
        ),
        DataType::LargeUtf8 => bytes(
            out,
            a.as_any()
                .downcast_ref::<LargeStringArray>()
                .map(|s| s.value(i).as_bytes())
                .unwrap_or_default(),
        ),
        DataType::Binary => bytes(
            out,
            a.as_any()
                .downcast_ref::<BinaryArray>()
                .map(|s| s.value(i))
                .unwrap_or_default(),
        ),
        DataType::LargeBinary => bytes(
            out,
            a.as_any()
                .downcast_ref::<LargeBinaryArray>()
                .map(|s| s.value(i))
                .unwrap_or_default(),
        ),
        other => {
            return Err(Error::InvalidSchema(format!(
                "key columns of type {other} have no canonical encoding"
            )));
        }
    }
    Ok(())
}

/// The canonical bytes of row `i` over `columns`.
pub fn encode_row(columns: &[ArrayRef], i: usize) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(16 * columns.len());
    for c in columns {
        encode_value(c.as_ref(), i, &mut out)?;
    }
    Ok(out)
}

/// The fingerprint of canonical key bytes.
pub fn fingerprint(key: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, Int64Array};
    use std::sync::Arc;

    fn enc(a: ArrayRef) -> Vec<Vec<u8>> {
        (0..a.len())
            .map(|i| encode_row(std::slice::from_ref(&a), i).unwrap())
            .collect()
    }

    fn sorted(v: &[Vec<u8>]) -> bool {
        v.windows(2).all(|w| w[0] <= w[1])
    }

    #[test]
    fn encodings_preserve_order() {
        assert!(sorted(&enc(Arc::new(Int64Array::from(vec![
            i64::MIN,
            -5,
            -1,
            0,
            1,
            7,
            i64::MAX
        ])))));
        assert!(sorted(&enc(Arc::new(Float64Array::from(vec![
            f64::NEG_INFINITY,
            -2.5,
            -0.0,
            0.0,
            1e-300,
            3.0,
            f64::INFINITY
        ])))));
        assert!(sorted(&enc(Arc::new(StringArray::from(vec![
            "", "\0", "\0\0", "a", "a\0", "aa", "b", "é"
        ])))));
        assert!(sorted(&enc(Arc::new(Int32Array::from(vec![
            None,
            Some(-1),
            Some(3)
        ])))));
        // Two columns: the first decides, then the second.
        let a: ArrayRef = Arc::new(StringArray::from(vec!["a", "a", "ab"]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![2, 10, 1]));
        let rows: Vec<Vec<u8>> = (0..3)
            .map(|i| encode_row(&[a.clone(), b.clone()], i).unwrap())
            .collect();
        assert!(sorted(&rows));
    }

    /// Pinned bytes: a change to the encoding fails here (and the key golden), not silently.
    #[test]
    fn pinned_bytes() {
        let a: ArrayRef = Arc::new(StringArray::from(vec!["r\0x"]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![-2]));
        let r = encode_row(&[a, b], 0).unwrap();
        assert_eq!(
            r,
            vec![
                5, b'r', 0, 0xFF, b'x', 0, 0, 1, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE
            ]
        );
        assert_eq!(fingerprint(&r), xxhash_rust::xxh3::xxh3_64(&r));
    }
}
