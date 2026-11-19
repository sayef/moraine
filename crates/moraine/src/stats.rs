//! Per-file column statistics: computed when moraine writes a file, stored in the `add` action
//! in Delta's JSON form (`numRecords`, `minValues`, `maxValues`, `nullCount`), and read back
//! into the snapshot so file pruning needs no request (D19).

use std::collections::HashMap;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use chrono::{DateTime, NaiveDate, Utc};
use datafusion::common::ScalarValue;
use serde_json::{Map, Value, json};

use crate::error::Result;

/// Longest string kept in statistics; longer values are cut (minimum) or cut and capped with
/// U+10FFFF (maximum), which keeps both valid bounds.
const MAX_STAT_CHARS: usize = 256;
/// Other Delta writers cut string statistics at 32 characters without a cap, so their maximum of
/// that length may be below the true maximum.
const FOREIGN_TRUNCATION: usize = 32;

/// Bounds of one column in one file (or one page).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ColRange {
    pub min: Option<ScalarValue>,
    pub max: Option<ScalarValue>,
    pub nulls: Option<u64>,
    pub rows: Option<u64>,
}

impl ColRange {
    /// Every value is null, so no comparison can match.
    pub fn all_null(&self) -> bool {
        matches!((self.nulls, self.rows), (Some(n), Some(r)) if n == r && r > 0)
    }
}

/// Statistics of one data file.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FileStats {
    pub rows: Option<u64>,
    pub columns: HashMap<String, ColRange>,
}

impl FileStats {
    pub fn column(&self, name: &str) -> Option<&ColRange> {
        self.columns.get(name)
    }

    /// The statistics of the rows of `self` and `other` together (a file written in chunks).
    pub fn merge(&mut self, other: &FileStats) {
        self.rows = match (self.rows, other.rows) {
            (Some(a), Some(b)) => Some(a + b),
            _ => None,
        };
        for (name, o) in &other.columns {
            let Some(c) = self.columns.get_mut(name) else {
                self.columns.insert(name.clone(), o.clone());
                continue;
            };
            let add = |a: Option<u64>, b: Option<u64>| a.zip(b).map(|(a, b)| a + b);
            c.nulls = add(c.nulls, o.nulls);
            c.rows = add(c.rows, o.rows);
            let pick = |a: &Option<ScalarValue>, b: &Option<ScalarValue>, less: bool| match (a, b) {
                (Some(x), Some(y)) => match x.partial_cmp(y) {
                    Some(std::cmp::Ordering::Less) => Some(if less { x } else { y }.clone()),
                    Some(_) => Some(if less { y } else { x }.clone()),
                    None => None,
                },
                (Some(x), None) | (None, Some(x)) => Some(x.clone()),
                (None, None) => None,
            };
            c.min = pick(&c.min, &o.min, true);
            c.max = pick(&c.max, &o.max, false);
        }
    }
}

/// Whether moraine keeps min and max for this type.
fn has_bounds(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Date32
            | DataType::Timestamp(TimeUnit::Microsecond, _)
    )
}

fn min_max(a: &ArrayRef) -> Result<(Option<ScalarValue>, Option<ScalarValue>)> {
    use datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
    use datafusion::logical_expr::Accumulator;
    let mut mn = MinAccumulator::try_new(a.data_type())?;
    let mut mx = MaxAccumulator::try_new(a.data_type())?;
    mn.update_batch(std::slice::from_ref(a))?;
    mx.update_batch(std::slice::from_ref(a))?;
    let some = |v: ScalarValue| (!v.is_null()).then_some(v);
    Ok((some(mn.evaluate()?), some(mx.evaluate()?)))
}

/// Statistics of `batch`, for every top-level column.
pub fn compute(batch: &RecordBatch) -> Result<FileStats> {
    let rows = batch.num_rows() as u64;
    let mut columns = HashMap::new();
    for (f, a) in batch.schema().fields().iter().zip(batch.columns()) {
        let mut r = ColRange {
            nulls: Some(a.null_count() as u64),
            rows: Some(rows),
            ..Default::default()
        };
        if has_bounds(f.data_type()) && a.null_count() < a.len() {
            let (mn, mx) = min_max(a)?;
            r.min = mn.map(cut_min);
            r.max = mx.map(cut_max);
        }
        columns.insert(f.name().clone(), r);
    }
    Ok(FileStats {
        rows: Some(rows),
        columns,
    })
}

fn cut_min(v: ScalarValue) -> ScalarValue {
    match v {
        ScalarValue::Utf8(Some(s)) if s.chars().count() > MAX_STAT_CHARS => {
            ScalarValue::Utf8(Some(s.chars().take(MAX_STAT_CHARS).collect()))
        }
        other => other,
    }
}

fn cut_max(v: ScalarValue) -> ScalarValue {
    match v {
        ScalarValue::Utf8(Some(s)) if s.chars().count() > MAX_STAT_CHARS => {
            let mut c: String = s.chars().take(MAX_STAT_CHARS).collect();
            c.push('\u{10FFFF}');
            ScalarValue::Utf8(Some(c))
        }
        other => other,
    }
}

/// Delta's JSON form of `stats`.
pub fn to_delta_json(stats: &FileStats, schema: &Schema) -> String {
    let mut mins = Map::new();
    let mut maxs = Map::new();
    let mut nulls = Map::new();
    for f in schema.fields() {
        let Some(r) = stats.columns.get(f.name()) else {
            continue;
        };
        if let Some(n) = r.nulls {
            nulls.insert(f.name().clone(), json!(n));
        }
        if let Some(v) = r.min.as_ref().and_then(|v| to_json(v, false)) {
            mins.insert(f.name().clone(), v);
        }
        if let Some(v) = r.max.as_ref().and_then(|v| to_json(v, true)) {
            maxs.insert(f.name().clone(), v);
        }
    }
    json!({
        "numRecords": stats.rows.unwrap_or(0),
        "minValues": mins,
        "maxValues": maxs,
        "nullCount": nulls,
    })
    .to_string()
}

fn to_json(v: &ScalarValue, is_max: bool) -> Option<Value> {
    Some(match v {
        ScalarValue::Int8(Some(x)) => json!(x),
        ScalarValue::Int16(Some(x)) => json!(x),
        ScalarValue::Int32(Some(x)) => json!(x),
        ScalarValue::Int64(Some(x)) => json!(x),
        ScalarValue::Float32(Some(x)) if x.is_finite() => json!(x),
        ScalarValue::Float64(Some(x)) if x.is_finite() => json!(x),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => json!(s),
        ScalarValue::Date32(Some(d)) => {
            let date = NaiveDate::from_num_days_from_ce_opt(*d + 719_163)?;
            json!(date.format("%Y-%m-%d").to_string())
        }
        ScalarValue::TimestampMicrosecond(Some(us), _) => {
            // Delta keeps milliseconds: round the minimum down and the maximum up.
            let ms = if is_max {
                us.div_euclid(1000) + i64::from(us.rem_euclid(1000) != 0)
            } else {
                us.div_euclid(1000)
            };
            let t = DateTime::<Utc>::from_timestamp_millis(ms)?;
            json!(t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        }
        _ => return None,
    })
}

/// Parses Delta's JSON statistics. `exact` is false for files moraine did not write: their
/// string maxima may be cut short and timestamps rounded, so such bounds are widened or dropped.
pub fn from_delta_json(text: Option<&str>, schema: &Schema, exact: bool) -> FileStats {
    let Some(v) = text.and_then(|t| serde_json::from_str::<Value>(t).ok()) else {
        return FileStats::default();
    };
    let rows = v.get("numRecords").and_then(Value::as_u64);
    let mut columns = HashMap::new();
    for f in schema.fields() {
        let get = |section: &str| v.get(section).and_then(|s| s.get(f.name()));
        let min = get("minValues").and_then(|x| from_json(x, f.data_type()));
        let mut max = get("maxValues").and_then(|x| from_json(x, f.data_type()));
        if !exact {
            max = max.and_then(widen_foreign_max);
        }
        let nulls = get("nullCount").and_then(Value::as_u64);
        columns.insert(
            f.name().clone(),
            ColRange {
                min,
                max,
                nulls,
                rows,
            },
        );
    }
    FileStats { rows, columns }
}

fn widen_foreign_max(m: ScalarValue) -> Option<ScalarValue> {
    match m {
        ScalarValue::Utf8(Some(s)) if s.chars().count() >= FOREIGN_TRUNCATION => None,
        ScalarValue::TimestampMicrosecond(Some(us), tz) => {
            Some(ScalarValue::TimestampMicrosecond(Some(us + 1000), tz))
        }
        other => Some(other),
    }
}

fn from_json(v: &Value, t: &DataType) -> Option<ScalarValue> {
    Some(match t {
        DataType::Int8 => ScalarValue::Int8(Some(i8::try_from(v.as_i64()?).ok()?)),
        DataType::Int16 => ScalarValue::Int16(Some(i16::try_from(v.as_i64()?).ok()?)),
        DataType::Int32 => ScalarValue::Int32(Some(i32::try_from(v.as_i64()?).ok()?)),
        DataType::Int64 => ScalarValue::Int64(Some(v.as_i64()?)),
        DataType::Float32 => ScalarValue::Float32(Some(v.as_f64()? as f32)),
        DataType::Float64 => ScalarValue::Float64(Some(v.as_f64()?)),
        DataType::Utf8 => ScalarValue::Utf8(Some(v.as_str()?.to_string())),
        DataType::LargeUtf8 => ScalarValue::LargeUtf8(Some(v.as_str()?.to_string())),
        DataType::Date32 => {
            let d = NaiveDate::parse_from_str(v.as_str()?, "%Y-%m-%d").ok()?;
            ScalarValue::Date32(Some(chrono::Datelike::num_days_from_ce(&d) - 719_163))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let t = DateTime::parse_from_rfc3339(v.as_str()?).ok()?;
            ScalarValue::TimestampMicrosecond(Some(t.timestamp_micros()), tz.clone())
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::Field;
    use std::sync::Arc;

    #[test]
    fn json_round_trip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("n", DataType::Int64, true),
            Field::new(
                "t",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));
        let b = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["b", "a", "c"])),
                Arc::new(Int64Array::from(vec![Some(5), None, Some(-2)])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![1_500, 2_000_001, 3]).with_timezone("UTC"),
                ),
            ],
        )
        .unwrap();
        let s = compute(&b).unwrap();
        let back = from_delta_json(Some(&to_delta_json(&s, &schema)), &schema, true);
        assert_eq!(back.rows, Some(3));
        let k = back.column("k").unwrap();
        assert_eq!(k.min, Some(ScalarValue::Utf8(Some("a".into()))));
        assert_eq!(k.max, Some(ScalarValue::Utf8(Some("c".into()))));
        assert_eq!(back.column("n").unwrap().nulls, Some(1));
        assert_eq!(
            back.column("n").unwrap().min,
            Some(ScalarValue::Int64(Some(-2)))
        );
        // Milliseconds, rounded outwards.
        let t = back.column("t").unwrap();
        assert_eq!(
            t.min,
            Some(ScalarValue::TimestampMicrosecond(
                Some(0),
                Some("UTC".into())
            ))
        );
        assert_eq!(
            t.max,
            Some(ScalarValue::TimestampMicrosecond(
                Some(2_001_000),
                Some("UTC".into())
            ))
        );
    }

    #[test]
    fn long_strings_keep_valid_bounds() {
        let long = "x".repeat(300);
        let lo = cut_min(ScalarValue::Utf8(Some(long.clone())));
        let hi = cut_max(ScalarValue::Utf8(Some(long.clone())));
        let v = ScalarValue::Utf8(Some(long));
        assert!(lo <= v && v <= hi);
    }
}
