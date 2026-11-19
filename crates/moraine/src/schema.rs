//! Names, schemas and incoming batches (cases X1 and D1 to D4).

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::row::{RowConverter, SortField};

use crate::error::{Error, Result};
use crate::names::NotFound;
use crate::options::TableDef;

/// Column names starting with this are reserved for moraine (case D4).
pub const RESERVED_PREFIX: &str = "_moraine";

/// Table names match `[a-z0-9][a-z0-9_-]{0,63}` (case X1).
pub fn validate_table_name(name: &str) -> Result<()> {
    let b = name.as_bytes();
    let ok_first = |c: u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    let ok_rest = |c: u8| ok_first(c) || c == b'_' || c == b'-';
    if !b.is_empty() && b.len() <= 64 && ok_first(b[0]) && b[1..].iter().all(|c| ok_rest(*c)) {
        Ok(())
    } else {
        Err(Error::InvalidSchema(format!(
            "table name {name:?} does not match [a-z0-9][a-z0-9_-]{{0,63}}"
        )))
    }
}

/// Column rules at creation: at least one column, no names that differ only by case, no
/// reserved prefix, key and cluster columns exist, are not nullable (keys) and are of a type
/// that has an order.
pub(crate) fn validate_new_schema(schema: &Schema, def: &TableDef) -> Result<()> {
    if schema.fields().is_empty() {
        return Err(Error::InvalidSchema(
            "a table needs at least one column".into(),
        ));
    }
    let mut lower = HashSet::new();
    for f in schema.fields() {
        if f.name().is_empty() {
            return Err(Error::InvalidSchema("empty column name".into()));
        }
        if f.name().starts_with(RESERVED_PREFIX) {
            return Err(Error::InvalidSchema(format!(
                "column {:?} uses the reserved prefix {RESERVED_PREFIX}",
                f.name()
            )));
        }
        if !lower.insert(f.name().to_lowercase()) {
            return Err(Error::InvalidSchema(format!(
                "column names differ only by case: {:?}",
                f.name()
            )));
        }
    }
    let mut seen = BTreeSet::new();
    for c in &def.primary_key {
        if !seen.insert(c) {
            return Err(Error::InvalidSchema(format!("key column {c} named twice")));
        }
    }
    for c in def.primary_key.iter().chain(&def.cluster_by) {
        let f = schema.field_with_name(c).map_err(|_| {
            Error::InvalidSchema(format!("key or cluster column {c} not in schema"))
        })?;
        if !orderable(f.data_type()) {
            return Err(Error::InvalidSchema(format!(
                "key or cluster column {c} has type {}, which has no order",
                f.data_type()
            )));
        }
    }
    for c in &def.primary_key {
        if schema.field_with_name(c).is_ok_and(|f| f.is_nullable()) {
            return Err(Error::InvalidSchema(format!(
                "primary key column {c} must not be nullable"
            )));
        }
    }
    Ok(())
}

/// Types a key or cluster column may have: scalars with a total order.
pub(crate) fn orderable(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
    )
}

/// Plain `Binary` values are limited to 16 MiB (case D11); larger payloads use the `Blob` type.
const MAX_INLINE_BINARY: usize = 16 << 20;

/// Brings `batch` to the table schema (case D3): columns are matched by name, a missing
/// nullable column is filled with nulls, an extra column, a missing non-nullable column or a
/// different type is [`Error::InvalidBatch`]. Timestamps with an equivalent time zone are cast
/// (case D12). Null keys are [`Error::InvalidBatch`] (case D1).
pub(crate) fn conform(
    batch: &RecordBatch,
    table: &SchemaRef,
    def: &TableDef,
) -> Result<RecordBatch> {
    let given = batch.schema();
    for f in given.fields() {
        if table.field_with_name(f.name()).is_err() {
            return Err(Error::InvalidBatch(format!(
                "column {:?} is not in the table",
                f.name()
            )));
        }
    }
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(table.fields().len());
    for tf in table.fields() {
        let col = match given.index_of(tf.name()) {
            Ok(i) => {
                let c = batch.column(i).clone();
                if c.data_type() == tf.data_type() {
                    c
                } else if same_timestamp(c.data_type(), tf.data_type())
                    || same_but_item_names(c.data_type(), tf.data_type())
                {
                    cast(&c, tf.data_type())?
                } else {
                    return Err(Error::InvalidBatch(format!(
                        "column {:?} has type {}, the table has {}",
                        tf.name(),
                        c.data_type(),
                        tf.data_type()
                    )));
                }
            }
            Err(_) if tf.is_nullable() => new_null_array(tf.data_type(), batch.num_rows()),
            Err(_) => {
                return Err(Error::InvalidBatch(format!(
                    "column {:?} is missing and not nullable",
                    tf.name()
                )));
            }
        };
        if !tf.is_nullable() && col.null_count() > 0 {
            let what = if def.primary_key.contains(tf.name()) {
                "primary key column"
            } else {
                "non-nullable column"
            };
            return Err(Error::InvalidBatch(format!(
                "{what} {:?} holds nulls",
                tf.name()
            )));
        }
        if matches!(tf.data_type(), DataType::Binary) {
            let b = col
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
                .ok_or_else(|| Error::Internal("binary column downcast".into()))?;
            if b.iter().flatten().any(|v| v.len() > MAX_INLINE_BINARY) {
                return Err(Error::InvalidBatch(format!(
                    "a value of binary column {:?} is above 16 MiB; use the Blob type",
                    tf.name()
                )));
            }
        }
        cols.push(col);
    }
    Ok(RecordBatch::try_new(table.clone(), cols)?)
}

/// Delta names a list's item field `element`; Arrow's default is `item` (D39).
pub const LIST_ITEM: &str = "element";

/// `t` with every list item field, at any depth, named [`LIST_ITEM`].
pub(crate) fn delta_item_names(t: &DataType) -> DataType {
    let item = |f: &Arc<Field>| {
        Arc::new(
            f.as_ref()
                .clone()
                .with_name(LIST_ITEM)
                .with_data_type(delta_item_names(f.data_type())),
        )
    };
    match t {
        DataType::List(f) => DataType::List(item(f)),
        DataType::LargeList(f) => DataType::LargeList(item(f)),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(item(f), *n),
        DataType::Struct(fs) => DataType::Struct(
            fs.iter()
                .map(|f| {
                    Arc::new(
                        f.as_ref()
                            .clone()
                            .with_data_type(delta_item_names(f.data_type())),
                    )
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Whether `a` and `b` differ at most in the names of list item fields.
pub(crate) fn same_but_item_names(a: &DataType, b: &DataType) -> bool {
    a != b && delta_item_names(a) == delta_item_names(b)
}

/// `schema` with every list item field named [`LIST_ITEM`] (D39).
pub(crate) fn with_delta_item_names(schema: &Schema) -> Schema {
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|f| {
                f.as_ref()
                    .clone()
                    .with_data_type(delta_item_names(f.data_type()))
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )
}

fn same_timestamp(a: &DataType, b: &DataType) -> bool {
    match (a, b) {
        (DataType::Timestamp(_, Some(x)), DataType::Timestamp(_, Some(y))) => {
            let utc = |z: &str| matches!(z, "UTC" | "+00:00" | "Z" | "Etc/UTC");
            utc(x) && utc(y)
        }
        (DataType::Timestamp(_, None), DataType::Timestamp(_, None)) => true,
        _ => false,
    }
}

/// Order-preserving byte encodings of the `columns` of `batch`, one per row (case D7: values
/// compare by value, strings and binary bytewise).
pub(crate) fn key_rows(batch: &RecordBatch, columns: &[String]) -> Result<arrow::row::Rows> {
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .map(|c| {
            batch
                .column_by_name(c)
                .cloned()
                .ok_or_else(|| Error::Internal(format!("key column {c} missing")))
        })
        .collect::<Result<_>>()?;
    let conv = RowConverter::new(
        arrays
            .iter()
            .map(|a| SortField::new(a.data_type().clone()))
            .collect(),
    )?;
    Ok(conv.convert_columns(&arrays)?)
}

/// [`Error::DuplicateKeyInBatch`] when two rows of `batches` share a key (case D2).
pub(crate) fn check_unique_keys(batches: &[RecordBatch], key: &[String]) -> Result<()> {
    check_unique_keys_with(batches, key, &mut HashSet::new())
}

/// [`check_unique_keys`] across calls: `seen` holds the keys of earlier batches.
pub(crate) fn check_unique_keys_with(
    batches: &[RecordBatch],
    key: &[String],
    seen: &mut HashSet<Vec<u8>>,
) -> Result<()> {
    if key.is_empty() {
        return Ok(());
    }
    for b in batches {
        let rows = key_rows(b, key)?;
        for (i, r) in rows.iter().enumerate() {
            if !seen.insert(r.as_ref().to_vec()) {
                let shown: Vec<String> = key
                    .iter()
                    .map(|c| {
                        b.column_by_name(c)
                            .and_then(|a| arrow::util::display::array_value_to_string(a, i).ok())
                            .unwrap_or_default()
                    })
                    .collect();
                return Err(Error::DuplicateKeyInBatch(shown.join(", ")));
            }
        }
    }
    Ok(())
}

/// [`Error::ColumnNotFound`] for `name`, naming the closest column of `schema` and all of them.
pub(crate) fn column_not_found(name: &str, schema: &Schema) -> Error {
    Error::ColumnNotFound(NotFound::among(
        name,
        schema.fields().iter().map(|f| f.name().as_str()),
    ))
}

/// A schema of the given columns of `schema`, in the given order.
pub(crate) fn project_schema(schema: &SchemaRef, columns: &[String]) -> Result<SchemaRef> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| {
            schema
                .field_with_name(c)
                .cloned()
                .map_err(|_| column_not_found(c, schema))
        })
        .collect::<Result<_>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

    fn def(pk: &[&str]) -> TableDef {
        crate::options::TableDef::from_properties(
            &crate::options::TableOptions::default()
                .primary_key(pk.iter().copied())
                .to_properties()
                .unwrap(),
        )
        .unwrap()
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, true),
        ]))
    }

    #[test]
    fn table_names_x1() {
        for ok in ["a", "runs", "a-b_c", "0x", &"a".repeat(64)] {
            validate_table_name(ok).unwrap();
        }
        for bad in [
            "",
            "A",
            "_a",
            "-a",
            "a/b",
            "..",
            "a b",
            &"a".repeat(65),
            "é",
        ] {
            assert!(validate_table_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn column_rules_d4() {
        let d = def(&[]);
        let case = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("A", DataType::Int64, true),
        ]);
        assert!(validate_new_schema(&case, &d).is_err());
        let reserved = Schema::new(vec![Field::new("_moraine_x", DataType::Int64, true)]);
        assert!(validate_new_schema(&reserved, &d).is_err());
        let nullable_key = Schema::new(vec![Field::new("k", DataType::Utf8, true)]);
        assert!(validate_new_schema(&nullable_key, &def(&["k"])).is_err());
    }

    #[test]
    fn conform_d3_d1() {
        let d = def(&["k"]);
        let t = schema();
        // Reordered and missing a nullable column: accepted.
        let only_k = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["a"]))],
        )
        .unwrap();
        let c = conform(&only_k, &t, &d).unwrap();
        assert_eq!(c.num_columns(), 2);
        assert_eq!(c.column(1).null_count(), 1);
        // Extra column.
        let extra = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Utf8, false),
                Field::new("x", DataType::Int64, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        assert!(matches!(
            conform(&extra, &t, &d),
            Err(Error::InvalidBatch(_))
        ));
        // Retyped.
        let retyped = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)])),
            vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
        )
        .unwrap();
        assert!(matches!(
            conform(&retyped, &t, &d),
            Err(Error::InvalidBatch(_))
        ));
        // Null key (D1).
        let null_key = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![None::<&str>]))],
        )
        .unwrap();
        assert!(matches!(
            conform(&null_key, &t, &d),
            Err(Error::InvalidBatch(_))
        ));
    }

    #[test]
    fn duplicate_keys_d2_d7() {
        let t = schema();
        let b = RecordBatch::try_new(
            t.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();
        assert!(matches!(
            check_unique_keys(&[b], &["k".into()]),
            Err(Error::DuplicateKeyInBatch(_))
        ));
        // D8: strings equal only after normalisation are different keys.
        let nfc = RecordBatch::try_new(
            t,
            vec![
                Arc::new(StringArray::from(vec!["\u{e9}", "e\u{301}"])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        check_unique_keys(&[nfc], &["k".into()]).unwrap();
    }
}
