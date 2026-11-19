//! Classic single-file Delta checkpoints, written and read by moraine itself (D47).
//!
//! A checkpoint is the table's state at one version as one Parquet file: one row per action,
//! with a column per action kind (`txn`, `add`, `remove`, `metaData`, `protocol`), as the Delta
//! protocol describes. moraine writes the actions as the JSON lines a log entry would hold and
//! decodes them into that schema with Arrow's JSON reader, and reads a checkpoint back by
//! encoding its rows as JSON lines again, so the log parser is the only place that knows what an
//! action means. delta-rs reads these checkpoints (the interoperability tests check it), and
//! moraine falls back to delta-rs for checkpoints its reader cannot decode (other writers'
//! multi-part or V2 checkpoints).

use std::sync::{Arc, LazyLock};

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde_json::Value;

use crate::error::{Error, Result};

fn string_map() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "key_value",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            false,
        )),
        false,
    )
}

fn string_list() -> DataType {
    DataType::List(Arc::new(Field::new("element", DataType::Utf8, true)))
}

/// The checkpoint schema: the action columns of the Delta protocol that moraine writes.
pub(crate) static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let s = |fields: Vec<Field>| DataType::Struct(Fields::from(fields));
    Arc::new(Schema::new(vec![
        Field::new(
            "txn",
            s(vec![
                Field::new("appId", DataType::Utf8, false),
                Field::new("version", DataType::Int64, false),
                Field::new("lastUpdated", DataType::Int64, true),
            ]),
            true,
        ),
        Field::new(
            "add",
            s(vec![
                Field::new("path", DataType::Utf8, false),
                Field::new("partitionValues", string_map(), false),
                Field::new("size", DataType::Int64, false),
                Field::new("modificationTime", DataType::Int64, false),
                Field::new("dataChange", DataType::Boolean, false),
                Field::new("stats", DataType::Utf8, true),
                Field::new("tags", string_map(), true),
            ]),
            true,
        ),
        Field::new(
            "remove",
            s(vec![
                Field::new("path", DataType::Utf8, false),
                Field::new("deletionTimestamp", DataType::Int64, true),
                Field::new("dataChange", DataType::Boolean, false),
                Field::new("extendedFileMetadata", DataType::Boolean, true),
                Field::new("partitionValues", string_map(), true),
                Field::new("size", DataType::Int64, true),
            ]),
            true,
        ),
        Field::new(
            "metaData",
            s(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("description", DataType::Utf8, true),
                Field::new(
                    "format",
                    s(vec![
                        Field::new("provider", DataType::Utf8, false),
                        Field::new("options", string_map(), true),
                    ]),
                    false,
                ),
                Field::new("schemaString", DataType::Utf8, false),
                Field::new("partitionColumns", string_list(), false),
                Field::new("configuration", string_map(), true),
                Field::new("createdTime", DataType::Int64, true),
            ]),
            true,
        ),
        Field::new(
            "protocol",
            s(vec![
                Field::new("minReaderVersion", DataType::Int32, false),
                Field::new("minWriterVersion", DataType::Int32, false),
                Field::new("readerFeatures", string_list(), true),
                Field::new("writerFeatures", string_list(), true),
            ]),
            true,
        ),
    ]))
});

/// Encodes action lines (`{"add": {...}}` and so on, one per line) as a checkpoint file.
pub(crate) fn encode(lines: &[String]) -> Result<Bytes> {
    let bad = |e: &dyn std::fmt::Display| Error::Internal(format!("checkpoint: {e}"));
    let mut decoder = arrow::json::ReaderBuilder::new(SCHEMA.clone())
        .with_batch_size(8192)
        .build_decoder()
        .map_err(|e| bad(&e))?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for line in lines {
        let mut bytes = line.as_bytes();
        while !bytes.is_empty() {
            let n = decoder.decode(bytes).map_err(|e| bad(&e))?;
            bytes = &bytes[n..];
            if n == 0
                && let Some(b) = decoder.flush().map_err(|e| bad(&e))?
            {
                batches.push(b);
            }
        }
    }
    if let Some(b) = decoder.flush().map_err(|e| bad(&e))? {
        batches.push(b);
    }
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()))
        .build();
    let mut out = Vec::new();
    let mut w = ArrowWriter::try_new(&mut out, SCHEMA.clone(), Some(props))?;
    for b in &batches {
        w.write(b)?;
    }
    w.close()?;
    Ok(Bytes::from(out))
}

/// Decodes a checkpoint file into its actions, one JSON object per non-empty row.
///
/// A corrupt file can make the Parquet or Arrow decoder panic (T13 found an embedded Arrow
/// schema with an unknown type, which arrow-ipc 59 reports with `unimplemented!`); that is
/// `Error::Corrupt` here, so opening the table falls back to delta-rs or fails cleanly.
pub(crate) fn decode(version: u64, body: Bytes) -> Result<Vec<Value>> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| decode_inner(version, body)))
        .unwrap_or_else(|p| {
            let msg = p
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| p.downcast_ref::<String>().cloned())
                .unwrap_or_default();
            Err(Error::corrupt(
                format!("checkpoint {version}"),
                format_args!("the decoder panicked: {msg}"),
            ))
        })
}

fn decode_inner(version: u64, body: Bytes) -> Result<Vec<Value>> {
    let bad = |e: &dyn std::fmt::Display| Error::corrupt(format!("checkpoint {version}"), e);
    let reader = ParquetRecordBatchReaderBuilder::try_new(body)
        .map_err(|e| bad(&e))?
        .with_batch_size(8192)
        .build()
        .map_err(|e| bad(&e))?;
    let mut out = Vec::new();
    for b in reader {
        let b = b.map_err(|e| bad(&e))?;
        // Only the columns moraine reads: other writers add more (`sidecar`, `*_parsed`).
        let keep: Vec<usize> = ["txn", "add", "metaData", "protocol"]
            .iter()
            .filter_map(|c| b.schema().index_of(c).ok())
            .collect();
        let b = b.project(&keep).map_err(|e| bad(&e))?;
        let mut buf = Vec::new();
        {
            let mut w = arrow::json::LineDelimitedWriter::new(&mut buf);
            w.write(&b).map_err(|e| bad(&e))?;
            w.finish().map_err(|e| bad(&e))?;
        }
        for line in buf.split(|c| *c == b'\n') {
            if line.is_empty() {
                continue;
            }
            let v: Value = serde_json::from_slice(line).map_err(|e| bad(&e))?;
            let Some(o) = v.as_object() else {
                return Err(bad(&"a row is not an object"));
            };
            for (k, x) in o {
                if !x.is_null() {
                    out.push(serde_json::json!({ k.clone(): x.clone() }));
                }
            }
        }
    }
    Ok(out)
}
