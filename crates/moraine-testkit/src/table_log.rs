//! Reads a table's committed history straight from a [`SimStore`], without moraine: the Delta
//! log entries in version order, with the rows each one added. The invariant checks compare
//! what clients were told with this, so a bug in moraine's own log reader cannot hide itself.

use arrow::array::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;

use crate::sim_store::SimStore;

/// One committed log entry.
#[derive(Clone, Debug)]
pub struct CommittedEntry {
    pub version: u64,
    /// Paths of the data files it added, relative to the table root.
    pub adds: Vec<String>,
    pub removes: Vec<String>,
    /// `(appId, version)` of its `txn` actions.
    pub txns: Vec<(String, i64)>,
    pub has_metadata: bool,
    /// Column names of the schema a `metaData` action sets, in order.
    pub columns: Vec<String>,
    /// The table properties a `metaData` action sets.
    pub configuration: Option<std::collections::BTreeMap<String, String>>,
    /// `dataChange` of its adds and removes (false for compaction).
    pub data_change: bool,
    /// Server time the entry was written.
    pub committed_at: chrono::DateTime<chrono::Utc>,
}

/// The log of the Delta table at `root` (a path inside the store, without trailing slash).
/// Panics on a gap or an unparsable entry: either is a broken invariant (C15) in itself.
pub fn read_log(store: &SimStore, root: &str) -> Vec<CommittedEntry> {
    read_entries(store, root, false)
}

/// Like [`read_log`], including entries log cleanup has deleted since.
pub fn read_log_all(store: &SimStore, root: &str) -> Vec<CommittedEntry> {
    read_entries(store, root, true)
}

fn read_entries(store: &SimStore, root: &str, deleted: bool) -> Vec<CommittedEntry> {
    let prefix = format!("{root}/_delta_log/");
    let mut out = Vec::new();
    for (path, versions) in store.archive() {
        let Some(a) = versions.last() else {
            continue;
        };
        if a.deleted && !deleted {
            continue;
        }
        let (body, committed_at) = (a.data.clone(), versions[0].created);
        let Some(name) = path.strip_prefix(&prefix) else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".json") else {
            continue;
        };
        let version: u64 = stem.parse().expect("log entry name");
        let mut e = CommittedEntry {
            version,
            adds: Vec::new(),
            removes: Vec::new(),
            txns: Vec::new(),
            has_metadata: false,
            columns: Vec::new(),
            configuration: None,
            data_change: false,
            committed_at,
        };
        for line in std::str::from_utf8(&body).expect("utf-8").lines() {
            if line.trim().is_empty() {
                continue;
            }
            let v: Value = serde_json::from_str(line).expect("log line is JSON");
            if let Some(a) = v.get("add") {
                e.adds
                    .push(a["path"].as_str().expect("add.path").to_string());
                e.data_change |= a["dataChange"].as_bool().unwrap_or(true);
            }
            if let Some(r) = v.get("remove") {
                e.removes
                    .push(r["path"].as_str().expect("remove.path").to_string());
                e.data_change |= r["dataChange"].as_bool().unwrap_or(true);
            }
            if let Some(t) = v.get("txn") {
                e.txns.push((
                    t["appId"].as_str().expect("txn.appId").to_string(),
                    t["version"].as_i64().expect("txn.version"),
                ));
            }
            if let Some(m) = v.get("metaData") {
                e.has_metadata = true;
                e.configuration = m["configuration"].as_object().map(|o| {
                    o.iter()
                        .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                        .collect()
                });
                let schema: Value = serde_json::from_str(
                    m["schemaString"].as_str().expect("metaData.schemaString"),
                )
                .expect("schemaString is JSON");
                e.columns = schema["fields"]
                    .as_array()
                    .expect("schema fields")
                    .iter()
                    .map(|f| f["name"].as_str().expect("field name").to_string())
                    .collect();
            }
        }
        out.push(e);
    }
    out.sort_by_key(|e| e.version);
    for (i, e) in out.iter().enumerate() {
        assert_eq!(
            e.version, i as u64,
            "log has a gap before version {}",
            e.version
        );
    }
    out
}

/// The rows of a Parquet object in the store.
pub fn read_parquet(store: &SimStore, path: &str) -> Vec<RecordBatch> {
    let body: Bytes = store
        .peek(path)
        .unwrap_or_else(|| panic!("data file {path} referenced by the log is missing"));
    ParquetRecordBatchReaderBuilder::try_new(body)
        .expect("parquet footer")
        .build()
        .expect("parquet reader")
        .collect::<Result<Vec<_>, _>>()
        .expect("parquet rows")
}

/// The rows of a Parquet object the store held at some point, deleted or not.
pub fn read_parquet_archived(store: &SimStore, path: &str) -> Vec<RecordBatch> {
    let body: Bytes = store
        .peek_archived(path)
        .unwrap_or_else(|| panic!("data file {path} was never written"));
    ParquetRecordBatchReaderBuilder::try_new(body)
        .expect("parquet footer")
        .build()
        .expect("parquet reader")
        .collect::<Result<Vec<_>, _>>()
        .expect("parquet rows")
}
