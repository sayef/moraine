//! [`CommitLayer`] on the Delta Lake log, using delta-rs (D8, D18).
//!
//! delta-rs supplies the action types and their serialisation, the conversion between Arrow
//! schemas and Delta schemas, and the loading of checkpoints. moraine writes each commit entry
//! itself with one conditional PUT and reads entries with GETs, because delta-rs's commit path
//! lists the log and runs post-commit work on every commit, which D18's request budget does not
//! allow. This is the only module that names a delta-rs type.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use deltalake::kernel::engine::arrow_conversion::{TryFromArrow, TryFromKernel};
use deltalake::kernel::{Add, CommitInfo, Remove, StructType, Transaction};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde_json::{Value, json};
use url::Url;

use super::{
    CheckpointState, CommitLayer, DataFile, Loaded, LogEntry, LogObject, LogObjectKind, NewEntry,
    TableMeta, TxnId, checkpoint,
};
use crate::commit::AttemptError;
use crate::error::{Error, Result};
use crate::names::NotFound;
use crate::stats;

/// The `add.tags` key naming the sort columns of a file moraine wrote (D19).
const SORTED_TAG: &str = "moraine.sortedBy";

/// The Delta log of one table.
#[derive(Debug)]
pub struct DeltaLog {
    store: Arc<dyn ObjectStore>,
    /// The table's data root (the Delta table root) inside `store`.
    root: Path,
    log: Path,
    /// The data root as a URL, for delta-rs when it loads a checkpoint.
    url: Url,
    /// Parallel requests when reading many entries.
    concurrency: usize,
}

impl DeltaLog {
    pub fn new(store: Arc<dyn ObjectStore>, root: Path, url: Url, concurrency: usize) -> Self {
        let log = root.clone().join("_delta_log");
        DeltaLog {
            store,
            root,
            log,
            url,
            concurrency: concurrency.max(1),
        }
    }

    fn entry_path(&self, version: u64) -> Path {
        self.log.clone().join(format!("{version:020}.json"))
    }

    pub(crate) fn parse(
        &self,
        version: u64,
        body: &[u8],
        schema: Option<&SchemaRef>,
    ) -> Result<LogEntry> {
        let text = std::str::from_utf8(body)
            .map_err(|e| Error::corrupt(format!("log entry {version}"), e))?;
        let values = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|line| {
                serde_json::from_str::<Value>(line)
                    .map_err(|e| Error::corrupt(format!("log entry {version}"), e))
            })
            .collect::<Result<Vec<Value>>>()?;
        parse_actions(version, values, schema)
    }

    async fn get_entry(&self, version: u64) -> Result<Option<Bytes>> {
        match self.store.get(&self.entry_path(version)).await {
            Ok(r) => Ok(Some(r.bytes().await?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Lists the log: JSON versions and complete single-file checkpoints.
    async fn list_log(&self, offset: Option<&Path>) -> Result<(Vec<u64>, Vec<u64>)> {
        let metas: Vec<_> = match offset {
            Some(o) => self.store.list_with_offset(Some(&self.log), o),
            None => self.store.list(Some(&self.log)),
        }
        .try_collect()
        .await?;
        let mut json = Vec::new();
        let mut checkpoints = Vec::new();
        for m in metas {
            let Some(name) = m.location.filename() else {
                continue;
            };
            let (stem, rest) = name.split_once('.').unwrap_or((name, ""));
            if stem.len() != 20 {
                continue;
            }
            let Ok(v) = stem.parse::<u64>() else {
                continue;
            };
            match rest {
                "json" => json.push(v),
                "checkpoint.parquet" => checkpoints.push(v),
                _ => {}
            }
        }
        json.sort_unstable();
        checkpoints.sort_unstable();
        Ok((json, checkpoints))
    }

    /// The version `_last_checkpoint` names; `None` when it is missing or unreadable.
    async fn last_checkpoint_hint(&self) -> Option<u64> {
        let path = self.log.clone().join("_last_checkpoint");
        let body = self.store.get(&path).await.ok()?.bytes().await.ok()?;
        let v: Value = serde_json::from_slice(&body).ok()?;
        v.get("version")?.as_u64()
    }

    /// Loads checkpoint `base` (if any) and the listed entries after it.
    async fn load_listed(&self, json: Vec<u64>, base: Option<u64>) -> Result<Loaded> {
        let base = match base {
            Some(c) => Some(self.load_checkpoint(c).await?),
            None => None,
        };
        let first = base.as_ref().map_or(0, |(c, ..)| c.0 + 1);
        // Every version from the base to the newest listed one. A listing may miss an entry
        // that exists (a local directory read while other processes create files is not a
        // snapshot), so gaps are read by GET like the rest; an entry that is really missing
        // fails below.
        let last = json.iter().copied().filter(|v| *v >= first).max();
        let wanted: Vec<u64> = match last {
            Some(l) => (first..=l).collect(),
            None => Vec::new(),
        };
        if base.is_none() && wanted.is_empty() {
            return Err(Error::corrupt("log", "no checkpoint and no log entry 0"));
        }
        let bodies: Vec<(u64, Bytes)> = futures::stream::iter(wanted.iter().copied())
            .map(|v| async move {
                match self.get_entry(v).await? {
                    Some(b) => Ok((v, b)),
                    None => Err(Error::corrupt(format!("log entry {v}"), "missing")),
                }
            })
            .buffered(self.concurrency)
            .try_collect()
            .await?;
        let mut schema = base.as_ref().map(|(c, ..)| c.1.schema.clone());
        let mut entries = Vec::with_capacity(bodies.len());
        for (v, b) in bodies {
            let e = self.parse(v, &b, schema.as_ref())?;
            if let Some(m) = &e.meta {
                schema = Some(m.schema.clone());
            }
            entries.push(e);
        }
        let (base, base_txns, base_txn_times) = match base {
            Some((c, txns, times)) => (Some(c), txns, times),
            None => (None, Vec::new(), Vec::new()),
        };
        Ok(Loaded {
            base,
            base_txns,
            base_txn_times,
            entries,
        })
    }

    /// Checkpoint `version`: moraine's own reader first, delta-rs for what it cannot decode.
    #[allow(clippy::type_complexity)]
    async fn load_checkpoint(
        &self,
        version: u64,
    ) -> Result<(
        (u64, Arc<TableMeta>, Vec<DataFile>),
        Vec<TxnId>,
        Vec<(String, i64)>,
    )> {
        let path = self
            .log
            .clone()
            .join(format!("{version:020}.checkpoint.parquet"));
        let body = self.store.get(&path).await?.bytes().await?;
        let own = parse_checkpoint(version, body);
        match own {
            Ok(LogEntry {
                meta: Some(meta),
                adds,
                txns,
                txn_times,
                ..
            }) => {
                let files = adds
                    .into_iter()
                    .map(|mut f| {
                        f.added_in = 0;
                        f
                    })
                    .collect();
                Ok(((version, meta, files), txns, txn_times))
            }
            _ => {
                let base = self.load_checkpoint_delta_rs(version).await?;
                let txns = self.checkpoint_txns(version).await?;
                Ok((base, txns, Vec::new()))
            }
        }
    }

    /// The `txn` actions of checkpoint `version`, from its `txn` column. delta-rs looks
    /// transactions up by application id but cannot list them, so moraine reads the column
    /// itself: one more GET of the checkpoint, only when a handle loads from one.
    async fn checkpoint_txns(&self, version: u64) -> Result<Vec<TxnId>> {
        use arrow::array::{Array, AsArray};
        use arrow::datatypes::Int64Type;
        use parquet::arrow::ProjectionMask;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let path = self
            .log
            .clone()
            .join(format!("{version:020}.checkpoint.parquet"));
        let bad = |e: &dyn std::fmt::Display| {
            Error::corrupt(format!("checkpoint {version}"), format_args!("txn: {e}"))
        };
        let body = self.store.get(&path).await?.bytes().await?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(body).map_err(|e| bad(&e))?;
        if builder.schema().field_with_name("txn").is_err() {
            return Ok(Vec::new());
        }
        let mask = ProjectionMask::columns(builder.parquet_schema(), ["txn"]);
        let batches: Vec<_> = builder
            .with_projection(mask)
            .build()
            .map_err(|e| bad(&e))?
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| bad(&e))?;
        let mut out = Vec::new();
        for b in batches {
            let Some(txn) = b.column_by_name("txn").and_then(|c| c.as_struct_opt()) else {
                return Err(bad(&"not a struct"));
            };
            let (Some(app), Some(ver)) =
                (txn.column_by_name("appId"), txn.column_by_name("version"))
            else {
                return Err(bad(&"appId or version missing"));
            };
            let (Some(app), Some(ver)) = (
                app.as_string_opt::<i32>(),
                ver.as_primitive_opt::<Int64Type>(),
            ) else {
                return Err(bad(&"appId or version has the wrong type"));
            };
            for i in 0..txn.len() {
                if txn.is_valid(i) && app.is_valid(i) && ver.is_valid(i) {
                    out.push(TxnId::new(app.value(i), ver.value(i)));
                }
            }
        }
        Ok(out)
    }

    /// The state at checkpoint `version`, loaded by delta-rs.
    async fn load_checkpoint_delta_rs(
        &self,
        version: u64,
    ) -> Result<(u64, Arc<TableMeta>, Vec<DataFile>)> {
        let table = deltalake::DeltaTableBuilder::from_url(self.url.clone())
            .map_err(|e| Error::Internal(format!("delta-rs: {e}")))?
            .with_storage_backend(self.store.clone(), self.url.clone())
            .with_version(version)
            .load()
            .await
            .map_err(|e| Error::corrupt(format!("checkpoint {version}"), e))?;
        let state = table
            .snapshot()
            .map_err(|e| Error::corrupt(format!("checkpoint {version}"), e))?;
        let meta_json = serde_json::to_value(state.metadata())
            .map_err(|e| Error::Internal(format!("metadata: {e}")))?;
        let meta = Arc::new(parse_metadata(version, &meta_json)?);
        let protocol = serde_json::to_value(state.protocol())
            .map_err(|e| Error::Internal(format!("protocol: {e}")))?;
        check_protocol(&protocol)?;
        let mut files = Vec::new();
        for f in state.log_data().iter() {
            let stats_text = f.stats();
            files.push(DataFile {
                path: f.path().to_string(),
                size: f.size().max(0) as u64,
                stats: stats::from_delta_json(stats_text.as_deref(), &meta.schema, false),
                modification_time: f.modification_time(),
                sorted_by: None,
                added_in: 0,
                tags: BTreeMap::new(),
            });
        }
        Ok((version, meta, files))
    }
}

/// Parses checkpoint `version` with moraine's own reader (D47): its rows as actions, as a log
/// entry holding the table's state at that version.
pub(crate) fn parse_checkpoint(version: u64, body: Bytes) -> Result<LogEntry> {
    checkpoint::decode(version, body).and_then(|values| parse_actions(version, values, None))
}

/// Parses the actions of log entry (or checkpoint) `version`. `schema` is the table schema in
/// force before it, for parsing statistics.
fn parse_actions(version: u64, values: Vec<Value>, schema: Option<&SchemaRef>) -> Result<LogEntry> {
    let mut entry = LogEntry {
        version,
        ..Default::default()
    };
    let mut protocol: Option<String> = None;
    // Metadata comes before adds in every writer's output, but do not depend on it.
    let mut adds: Vec<Add> = Vec::new();
    for v in values {
        let Some((kind, body)) = v.as_object().and_then(|o| o.iter().next()) else {
            return Err(Error::corrupt(
                format!("log entry {version}"),
                "a line is not an action",
            ));
        };
        let bad = |e: serde_json::Error| Error::corrupt(format!("log entry {version}"), e);
        match kind.as_str() {
            "add" => {
                let a: Add = serde_json::from_value(body.clone()).map_err(bad)?;
                entry.data_change |= a.data_change;
                adds.push(a);
            }
            "remove" => {
                let r: Remove = serde_json::from_value(body.clone()).map_err(bad)?;
                entry.data_change |= r.data_change;
                entry.removes.push(r.path);
            }
            "txn" => {
                let t: Transaction = serde_json::from_value(body.clone()).map_err(bad)?;
                if let Some(at) = t.last_updated {
                    entry.txn_times.push((t.app_id.clone(), at));
                }
                entry.txns.push(TxnId {
                    app_id: t.app_id,
                    version: t.version,
                });
            }
            "metaData" => entry.meta = Some(Arc::new(parse_metadata(version, body)?)),
            "protocol" => {
                check_protocol(body)?;
                let own = body.get("minReaderVersion").and_then(Value::as_i64) == Some(1)
                    && body.get("minWriterVersion").and_then(Value::as_i64) == Some(2);
                if !own {
                    protocol = Some(body.to_string());
                }
            }
            // commitInfo, cdc, domainMetadata and anything newer carry nothing moraine
            // reads; a table feature that matters shows up in the protocol action.
            _ => {}
        }
    }
    if let (Some(p), Some(m)) = (protocol, entry.meta.as_mut()) {
        Arc::make_mut(m).protocol = Some(p);
    }
    let schema = entry
        .meta
        .as_ref()
        .map(|m| m.schema.clone())
        .or_else(|| schema.cloned());
    for a in adds {
        entry.adds.push(data_file(a, schema.as_ref(), version));
    }
    Ok(entry)
}

/// The `metaData` action line of `m`.
fn meta_line(m: &TableMeta, created: i64) -> Result<String> {
    let kernel = StructType::try_from_arrow(m.schema.as_ref())
        .map_err(|e| Error::InvalidSchema(e.to_string()))?;
    let schema_string =
        serde_json::to_string(&kernel).map_err(|e| Error::Internal(format!("schema: {e}")))?;
    let config: BTreeMap<&String, &String> = m.configuration.iter().collect();
    Ok(json!({"metaData": {
        "id": m.id,
        "format": {"provider": "parquet", "options": {}},
        "schemaString": schema_string,
        "partitionColumns": m.partition_columns,
        "configuration": config,
        "createdTime": created,
    }})
    .to_string())
}

/// The `add` action line of `f`.
fn add_line(f: &DataFile, schema: &Schema, data_change: bool) -> Result<String> {
    let mut tags: HashMap<String, Option<String>> = f
        .tags
        .iter()
        .map(|(k, v)| (k.clone(), Some(v.clone())))
        .collect();
    if let Some(cols) = &f.sorted_by {
        tags.insert(
            SORTED_TAG.to_string(),
            Some(serde_json::to_string(cols).unwrap_or_default()),
        );
    }
    let tags = (!tags.is_empty()).then_some(tags);
    let add = Add {
        path: f.path.clone(),
        partition_values: HashMap::new(),
        size: f.size as i64,
        modification_time: f.modification_time,
        data_change,
        stats: Some(stats::to_delta_json(&f.stats, schema)),
        tags,
        ..Default::default()
    };
    erased::Ser::to_line(&("add", &add))
}

fn data_file(a: Add, schema: Option<&SchemaRef>, version: u64) -> DataFile {
    let sorted_by = a
        .tags
        .as_ref()
        .and_then(|t| t.get(SORTED_TAG))
        .and_then(|v| v.as_deref())
        .and_then(|v| serde_json::from_str::<Vec<String>>(v).ok());
    let tags: BTreeMap<String, String> = a
        .tags
        .iter()
        .flatten()
        .filter(|(k, _)| k.as_str() != SORTED_TAG)
        .filter_map(|(k, v)| v.as_ref().map(|v| (k.clone(), v.clone())))
        .collect();
    let stats = match schema {
        Some(s) => stats::from_delta_json(a.stats.as_deref(), s, sorted_by.is_some()),
        None => Default::default(),
    };
    DataFile {
        path: a.path,
        size: a.size.max(0) as u64,
        stats,
        modification_time: a.modification_time,
        sorted_by,
        added_in: version,
        tags,
    }
}

fn parse_metadata(version: u64, v: &Value) -> Result<TableMeta> {
    let bad = |what: &str| {
        Error::corrupt(
            format!("log entry {version}"),
            format_args!("metaData {what}"),
        )
    };
    let id = v
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| bad("without id"))?
        .to_string();
    let schema_string = v
        .get("schemaString")
        .and_then(Value::as_str)
        .ok_or_else(|| bad("without schemaString"))?;
    let kernel: StructType = serde_json::from_str(schema_string)
        .map_err(|e| Error::corrupt(format!("log entry {version}"), format_args!("schema: {e}")))?;
    let schema = Schema::try_from_kernel(&kernel)
        .map_err(|e| Error::UnsupportedTableFeature(format!("schema: {e}")))?;
    let partition_columns = v
        .get("partitionColumns")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let configuration = v
        .get("configuration")
        .and_then(Value::as_object)
        .map(|o| {
            o.iter()
                .filter_map(|(k, x)| x.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();
    Ok(TableMeta {
        id,
        schema: Arc::new(schema),
        partition_columns,
        configuration,
        protocol: None,
    })
}

/// Refuses tables that need a reader or writer feature moraine does not implement (spike item
/// 11, case S8): deletion vectors, column mapping, constraints, generated columns, and so on.
fn check_protocol(v: &Value) -> Result<()> {
    let num = |k: &str| v.get(k).and_then(Value::as_i64).unwrap_or(1);
    let features = |k: &str| -> Vec<String> {
        v.get(k)
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };
    let (reader, writer) = (num("minReaderVersion"), num("minWriterVersion"));
    let reader_ok = reader <= 1 || (reader == 3 && features("readerFeatures").is_empty());
    let writer_features = features("writerFeatures");
    let writer_ok =
        writer <= 2 || (writer == 7 && writer_features.iter().all(|f| f == "appendOnly"));
    if reader_ok && writer_ok {
        Ok(())
    } else {
        Err(Error::UnsupportedTableFeature(format!(
            "protocol reader {reader} {:?}, writer {writer} {writer_features:?}",
            features("readerFeatures")
        )))
    }
}

#[async_trait]
impl CommitLayer for DeltaLog {
    fn canonical_schema(&self, schema: &Schema) -> Result<SchemaRef> {
        // List item fields take Delta's name whatever the caller named them (D39).
        let schema = &crate::schema::with_delta_item_names(schema);
        let kernel = StructType::try_from_arrow(schema)
            .map_err(|e| Error::InvalidSchema(format!("not representable in Delta: {e}")))?;
        let back = Schema::try_from_kernel(&kernel)
            .map_err(|e| Error::InvalidSchema(format!("not representable in Delta: {e}")))?;
        for (a, b) in schema.fields().iter().zip(back.fields()) {
            if a.data_type() != b.data_type() {
                return Err(Error::InvalidSchema(format!(
                    "column {:?}: type {} is stored as {}; declare it as {}",
                    a.name(),
                    a.data_type(),
                    b.data_type(),
                    b.data_type()
                )));
            }
        }
        Ok(Arc::new(back))
    }

    fn parse_entry(&self, version: u64, body: &[u8]) -> Result<LogEntry> {
        self.parse(version, body, None)
    }

    fn encode(&self, entry: &NewEntry, schema: &Schema) -> Result<Bytes> {
        let mut lines: Vec<String> = Vec::new();
        let enc = |v: &dyn erased::Ser| v.to_line();
        let info = CommitInfo {
            timestamp: Some(entry.timestamp),
            operation: Some(entry.operation.to_string()),
            operation_parameters: Some(HashMap::from([(
                "mode".to_string(),
                json!(match entry.operation {
                    "WRITE" => "Append",
                    "CREATE TABLE" => "ErrorIfExists",
                    _ => "Overwrite",
                }),
            )])),
            is_blind_append: Some(
                entry.meta.is_none() && entry.removes.is_empty() && entry.operation == "WRITE",
            ),
            engine_info: Some(format!("moraine/{}", env!("CARGO_PKG_VERSION"))),
            ..Default::default()
        };
        lines.push(enc(&("commitInfo", &info))?);
        if entry.protocol {
            lines.push(
                json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}).to_string(),
            );
        }
        if let Some(m) = &entry.meta {
            lines.push(meta_line(m, entry.timestamp)?);
        }
        if let Some(t) = &entry.txn {
            let txn =
                Transaction::new_with_last_update(&t.app_id, t.version, Some(entry.timestamp));
            lines.push(enc(&("txn", &txn))?);
        }
        for t in &entry.caller_txns {
            // No `lastUpdated`: Delta readers and checkpoint writers never expire a `txn`
            // without one, whatever `delta.setTransactionRetentionDuration` says (D32).
            let txn = Transaction::new_with_last_update(&t.app_id, t.version, None);
            lines.push(enc(&("txn", &txn))?);
        }
        for f in &entry.adds {
            lines.push(add_line(f, schema, entry.data_change)?);
        }
        for f in &entry.removes {
            let remove = Remove {
                path: f.path.clone(),
                data_change: entry.data_change,
                deletion_timestamp: Some(entry.timestamp),
                extended_file_metadata: Some(true),
                partition_values: Some(HashMap::new()),
                size: Some(f.size as i64),
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
            };
            lines.push(enc(&("remove", &remove))?);
        }
        let mut out = lines.join("\n");
        out.push('\n');
        Ok(Bytes::from(out))
    }

    async fn put_entry(&self, version: u64, body: Bytes) -> Result<(), AttemptError> {
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&self.entry_path(version), PutPayload::from(body), opts)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(AttemptError::from_store(e)),
        }
    }

    async fn read_entry(
        &self,
        version: u64,
        schema: Option<&SchemaRef>,
    ) -> Result<Option<LogEntry>> {
        match self.get_entry(version).await? {
            Some(body) => Ok(Some(self.parse(version, &body, schema)?)),
            None => Ok(None),
        }
    }

    async fn load(&self) -> Result<Loaded> {
        match self.load_once().await {
            // A checkpoint deleted between the listing and its read (a newer one replaced it,
            // D66): the next listing no longer names it.
            Err(e) if e.is_missing_object() => self.load_once().await,
            r => r,
        }
    }

    async fn load_at(&self, version: u64) -> Result<Loaded> {
        match self.load_at_once(version).await {
            Err(e) if e.is_missing_object() => Err(Error::VersionNotAvailable(version)),
            r => r,
        }
    }

    async fn scrub_entry(&self, version: u64, erased: &HashSet<String>) -> Result<bool> {
        let path = self.entry_path(version);
        // Another writer of the entry can only be log cleanup (deleting it) or another scrub
        // (writing the same result): a conditional put keeps a deleted entry from coming back.
        for _ in 0..8 {
            let got = match self.store.get(&path).await {
                Ok(g) => g,
                Err(object_store::Error::NotFound { .. }) => return Ok(false),
                Err(e) => return Err(e.into()),
            };
            let (e_tag, obj_version) = (got.meta.e_tag.clone(), got.meta.version.clone());
            let body = got.bytes().await?;
            let Some(scrubbed) = scrub(version, &body, erased)? else {
                return Ok(false);
            };
            let mode = match e_tag {
                Some(e_tag) => PutMode::Update(object_store::UpdateVersion {
                    e_tag: Some(e_tag),
                    version: obj_version,
                }),
                None => PutMode::Overwrite,
            };
            let opts = PutOptions {
                mode,
                ..Default::default()
            };
            match self
                .store
                .put_opts(&path, PutPayload::from(scrubbed.clone()), opts)
                .await
            {
                Ok(_) => return Ok(true),
                // Changed meanwhile: read it again.
                Err(object_store::Error::Precondition { .. })
                | Err(object_store::Error::AlreadyExists { .. }) => continue,
                Err(object_store::Error::NotFound { .. }) => return Ok(false),
                // A store without conditional updates: overwrite, under the purge lease.
                Err(object_store::Error::NotImplemented { .. })
                | Err(object_store::Error::NotSupported { .. }) => {
                    self.store.put(&path, PutPayload::from(scrubbed)).await?;
                    return Ok(true);
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(Error::TooManyConflicts { attempts: 8 })
    }

    async fn list_versions_after(&self, version: u64) -> Result<Vec<u64>> {
        let offset = self.entry_path(version);
        let (json, checkpoints) = self.list_log(Some(&offset)).await?;
        // Checkpoints count too: log cleanup may have removed every entry before the newest
        // checkpoint, and a listing of entries alone would then say "nothing new".
        let mut out: Vec<u64> = json
            .into_iter()
            .chain(checkpoints)
            .filter(|v| *v > version)
            .collect();
        out.sort_unstable();
        out.dedup();
        Ok(out)
    }

    fn data_prefix(&self) -> &Path {
        &self.root
    }

    async fn read_entry_at(
        &self,
        version: u64,
        schema: Option<&SchemaRef>,
    ) -> Result<Option<(LogEntry, chrono::DateTime<chrono::Utc>)>> {
        match self.store.get(&self.entry_path(version)).await {
            Ok(r) => {
                let at = r.meta.last_modified;
                let body = r.bytes().await?;
                Ok(Some((self.parse(version, &body, schema)?, at)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn write_checkpoint(&self, state: &CheckpointState<'_>) -> Result<bool> {
        let schema = state.meta.schema.as_ref();
        let mut lines = Vec::with_capacity(state.files.len() + state.txns.len() + 2);
        lines.push(match &state.meta.protocol {
            Some(p) => format!("{{\"protocol\":{p}}}"),
            None => json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}).to_string(),
        });
        lines.push(meta_line(state.meta, 0)?);
        for (app, version, at) in &state.txns {
            let txn = Transaction::new_with_last_update(app, *version, *at);
            lines.push(erased::Ser::to_line(&("txn", &txn))?);
        }
        for f in state.files {
            lines.push(add_line(f, schema, false)?);
        }
        let body = checkpoint::encode(&lines)?;
        let size = body.len();
        let path = self
            .log
            .clone()
            .join(format!("{:020}.checkpoint.parquet", state.version));
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        let created = match self
            .store
            .put_opts(&path, PutPayload::from(body), opts)
            .await
        {
            Ok(_) => true,
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. }) => false,
            Err(e) => return Err(e.into()),
        };
        if created {
            let hint = json!({
                "version": state.version,
                "size": lines.len(),
                "sizeInBytes": size,
                "numOfAddFiles": state.files.len(),
            });
            self.store
                .put(
                    &self.log.clone().join("_last_checkpoint"),
                    PutPayload::from(hint.to_string().into_bytes()),
                )
                .await?;
        }
        Ok(created)
    }

    async fn list_log_objects(&self) -> Result<Vec<LogObject>> {
        let metas: Vec<_> = self.store.list(Some(&self.log)).try_collect().await?;
        let mut out = Vec::new();
        for m in metas {
            let Some(name) = m.location.filename() else {
                continue;
            };
            let (stem, rest) = name.split_once('.').unwrap_or((name, ""));
            let Ok(version) = stem.parse::<u64>() else {
                continue;
            };
            if stem.len() != 20 {
                continue;
            }
            let kind = match rest {
                "json" => LogObjectKind::Entry,
                "checkpoint.parquet" => LogObjectKind::Checkpoint,
                _ => LogObjectKind::Other,
            };
            out.push(LogObject {
                location: m.location,
                version,
                kind,
                last_modified: m.last_modified,
            });
        }
        out.sort_by_key(|o| (o.version, o.kind as u8));
        Ok(out)
    }

    async fn delete_log_objects(&self, locations: Vec<Path>) -> Result<usize> {
        let mut n = 0;
        for l in locations {
            match self.store.delete(&l).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => n += 1,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(n)
    }
}

impl DeltaLog {
    async fn load_once(&self) -> Result<Loaded> {
        // `_last_checkpoint` names a recent checkpoint, so the listing can start there instead of
        // at version 0 (D47). A missing, unreadable or stale hint costs a full listing, never
        // correctness: the newest checkpoint in the listing wins either way.
        if let Some(hint) = self.last_checkpoint_hint().await {
            let offset = self.log.clone().join(format!("{hint:020}.checkpoint"));
            let (json, checkpoints) = self.list_log(Some(&offset)).await?;
            if let Some(&c) = checkpoints.iter().filter(|v| **v >= hint).max() {
                return self.load_listed(json, Some(c)).await;
            }
        }
        let (json, checkpoints) = self.list_log(None).await?;
        if json.is_empty() && checkpoints.is_empty() {
            return Err(Error::TableNotFound(NotFound::bare(self.root.to_string())));
        }
        let latest = json.iter().chain(&checkpoints).max().copied().unwrap_or(0);
        let base = checkpoints.iter().copied().filter(|c| *c <= latest).max();
        self.load_listed(json, base).await
    }

    async fn load_at_once(&self, version: u64) -> Result<Loaded> {
        let (json, checkpoints) = self.list_log(None).await?;
        let json: Vec<u64> = json.into_iter().filter(|v| *v <= version).collect();
        let base = checkpoints.into_iter().filter(|c| *c <= version).max();
        let loaded = match self.load_listed(json, base).await {
            Err(Error::Corrupt { .. }) => return Err(Error::VersionNotAvailable(version)),
            other => other?,
        };
        let last = loaded
            .entries
            .last()
            .map(|e| e.version)
            .or(loaded.base.as_ref().map(|b| b.0));
        if last != Some(version) {
            return Err(Error::VersionNotAvailable(version));
        }
        Ok(loaded)
    }
}

/// Log entry `body` with the values of erased files taken out (D66): the minimum and maximum
/// from the statistics of the files in `erased` it adds (row and null counts stay; a reader
/// only prunes less), any statistics of its removes, and the commit information other writers
/// may fill with values (operation parameters other than `mode`, user metadata). `None` when
/// there is nothing to take out. Other lines are kept byte for byte.
pub(crate) fn scrub(version: u64, body: &[u8], erased: &HashSet<String>) -> Result<Option<Bytes>> {
    let text =
        std::str::from_utf8(body).map_err(|e| Error::corrupt(format!("log entry {version}"), e))?;
    let bad = |e: serde_json::Error| Error::corrupt(format!("log entry {version}"), e);
    let mut out = String::with_capacity(text.len());
    let mut changed = false;
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let mut doc: Value = serde_json::from_str(line).map_err(bad)?;
        let mut edited = false;
        if let Some(add) = doc.get_mut("add").and_then(Value::as_object_mut) {
            let erase = add
                .get("path")
                .and_then(Value::as_str)
                .is_some_and(|p| erased.contains(p) || erased.contains(&percent_decode(p)));
            if erase && let Some(text) = add.get("stats").and_then(Value::as_str) {
                let mut st: Value = serde_json::from_str(text).map_err(bad)?;
                if let Some(o) = st.as_object_mut() {
                    let had = o.remove("minValues").is_some() | o.remove("maxValues").is_some();
                    if had {
                        add.insert("stats".into(), Value::String(st.to_string()));
                        edited = true;
                    }
                }
            }
        }
        if let Some(rm) = doc.get_mut("remove").and_then(Value::as_object_mut) {
            edited |= rm.remove("stats").is_some();
        }
        if let Some(ci) = doc.get_mut("commitInfo").and_then(Value::as_object_mut) {
            edited |= ci.remove("userMetadata").is_some();
            if let Some(p) = ci
                .get_mut("operationParameters")
                .and_then(Value::as_object_mut)
            {
                let before = p.len();
                p.retain(|k, _| k == "mode");
                edited |= p.len() != before;
            }
        }
        if edited {
            changed = true;
            out.push_str(&doc.to_string());
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    Ok(changed.then(|| Bytes::from(out)))
}

/// A path as an `add` action may write it (percent-encoded), decoded.
fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16)
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// One JSON line per action, `{"<kind>": {...}}`.
mod erased {
    use crate::error::{Error, Result};

    pub trait Ser {
        fn to_line(&self) -> Result<String>;
    }

    impl<T: serde::Serialize> Ser for (&str, &T) {
        fn to_line(&self) -> Result<String> {
            let body = serde_json::to_value(self.1)
                .map_err(|e| Error::Internal(format!("encode {}: {e}", self.0)))?;
            Ok(serde_json::json!({ self.0: body }).to_string())
        }
    }
}
