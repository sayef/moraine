//! The tracker workloads on moraine tables (DECISIONS.md D19 to D22), in two layouts.
//!
//! - `runs`, `params` and `series` are small tables keyed by run id: start-up reads each once per
//!   version and filters in memory (D20).
//! - `metrics` holds the points. In kv0's layout ([`Layout::Kv0`], the headline of the fair
//!   comparison) a point carries only a series id hashed from run, name and context, with no run
//!   id column and no clustering, as kv0 stores it. In moraine's own layout ([`Layout::Own`])
//!   points carry the run id and are clustered by (run id, series id); run ids are time-ordered,
//!   so one flush writes one file whose run-id range touches no other file, and a series read
//!   prunes to that file and then to its pages (D19).
//! - Records tables are keyed by (run id, item id), clustered by run id; a run-less record has
//!   the empty run id. Fresh keys are appended, rewritten keys upserted. Reads return every
//!   column with the data parsed from JSON, as kv0's do.
//!
//! A flush commits the four tracker tables concurrently: one data PUT and one log PUT each
//! (D18). kv0 commits its tables one after another; neither is atomic across tables.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{
    Array, AsArray, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef, TimeUnit};
use moraine::expr::{ScalarValue, col, lit};
use moraine::{Consistency, Database, Error, Table, TableOptions};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::engine::{Layout, Rec, Result, RunLog};

fn ts() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

fn runs_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("experiment", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, false),
        Field::new("created_at", ts(), false),
        Field::new("seq", DataType::Int64, false),
    ]))
}

fn params_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("num", DataType::Float64, true),
        Field::new("seq", DataType::Int64, false),
    ]))
}

fn series_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("series_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("context", DataType::Utf8, false),
        Field::new("seq", DataType::Int64, false),
    ]))
}

fn metrics_schema(layout: Layout) -> SchemaRef {
    Arc::new(Schema::new(match layout {
        Layout::Own => vec![
            Field::new("run_id", DataType::Utf8, false),
            Field::new("series_id", DataType::Int64, false),
            Field::new("step", DataType::Int64, false),
            Field::new("timestamp", ts(), false),
            Field::new("value", DataType::Float64, false),
        ],
        // kv0's metrics columns (its reserved statistics column aside).
        Layout::Kv0 => vec![
            Field::new("series_id", DataType::Int64, false),
            Field::new("step", DataType::Int64, false),
            Field::new("epoch", DataType::Int64, true),
            Field::new("timestamp", ts(), false),
            Field::new("value", DataType::Float64, false),
        ],
    }))
}

fn records_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("item_id", DataType::Utf8, false),
        Field::new("data", DataType::Utf8, false),
        Field::new("seq", DataType::Int64, false),
    ]))
}

fn keyed(pk: &[&str], cluster: &[&str]) -> TableOptions {
    TableOptions::default()
        .primary_key(pk.iter().copied())
        .enforce_primary_key(false)
        .cluster_by(cluster.iter().copied())
}

/// A stable 63-bit id of a series (FNV-1a). moraine's layout hashes name and context (the run
/// id is a column of its own); kv0's layout hashes the run id in as well, as kv0 does.
fn series_id(layout: Layout, run_id: &str, name: &str, context: &str) -> i64 {
    let run = match layout {
        Layout::Kv0 => run_id,
        Layout::Own => "",
    };
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in run
        .bytes()
        .chain([0u8])
        .chain(name.bytes())
        .chain([0u8])
        .chain(context.bytes())
    {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    (h >> 1) as i64
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

fn context_json(subset: &str) -> String {
    serde_json::json!({ "subset": subset }).to_string()
}

pub struct Repo {
    db: Database,
    layout: Layout,
    runs: Table,
    params: Table,
    series: Table,
    metrics: Table,
    records: Mutex<HashMap<String, Table>>,
    seq: AtomicI64,
    run_counter: AtomicU64,
    /// Queued write mode (D51): fresh records go to the inbox, and each handle runs an
    /// ingest loop that commits them while it holds the ingest lease.
    queued: bool,
    ingesting: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

async fn table(db: &Database, name: &str, schema: SchemaRef, opts: TableOptions) -> Result<Table> {
    match db.open_table(name).await {
        Ok(t) => Ok(t),
        Err(Error::TableNotFound(_)) => Ok(db.create_table(name, schema, opts).await?),
        Err(e) => Err(e.into()),
    }
}

impl Repo {
    pub async fn open(
        location: &str,
        storage: &HashMap<String, String>,
        layout: Layout,
        strong: bool,
        queued: bool,
    ) -> Result<Repo> {
        let mut opts = storage.clone();
        if let Some(v) = opts.remove("allow_http") {
            opts.insert("aws_allow_http".into(), v);
        }
        let mut b = Database::builder(location).storage_options(opts);
        if strong {
            b = b.consistency(Consistency::Strong);
        }
        let db = b.open().await?;
        let (by_run, metrics_opts) = match layout {
            Layout::Own => (
                vec!["run_id"],
                TableOptions::default().cluster_by(["run_id", "series_id"]),
            ),
            // Sorted by (series, step) as kv0's optimize sorts its points (D54): moraine sorts
            // every file it writes by its cluster key, and compaction merges in that order.
            Layout::Kv0 => (
                Vec::new(),
                TableOptions::default().cluster_by(["series_id", "step"]),
            ),
        };
        let (runs, params, series, metrics) = tokio::try_join!(
            table(&db, "runs", runs_schema(), keyed(&["run_id"], &by_run)),
            table(
                &db,
                "params",
                params_schema(),
                keyed(&["run_id", "key"], &by_run)
            ),
            table(
                &db,
                "series",
                series_schema(),
                keyed(&["run_id", "series_id"], &by_run)
            ),
            table(&db, "metrics", metrics_schema(layout), metrics_opts),
        )?;
        Ok(Repo {
            db,
            layout,
            runs,
            params,
            series,
            metrics,
            records: Mutex::new(HashMap::new()),
            seq: AtomicI64::new(now_micros() * 16),
            run_counter: AtomicU64::new(0),
            queued,
            ingesting: Mutex::new(Vec::new()),
        })
    }

    fn next_seq(&self) -> i64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Time-ordered run ids: microseconds since the epoch, then a per-process counter and a
    /// random part, all hex. Lexicographic order is creation order.
    fn new_run_id(&self) -> String {
        let n = self.run_counter.fetch_add(1, Ordering::Relaxed);
        let r = std::collections::hash_map::RandomState::new();
        let salt = std::hash::BuildHasher::hash_one(&r, n) & 0xffff_ffff;
        format!("{:014x}{:06x}{:08x}", now_micros(), n & 0xff_ffff, salt)
    }

    /// One run: building the four batches is the logging calls, the four concurrent commits
    /// the flush.
    pub async fn log_run(&self, run: &RunLog) -> Result<(Duration, Duration)> {
        let t0 = Instant::now();
        let run_id = self.new_run_id();
        let now = now_micros();
        let seq = self.next_seq();
        let runs = RecordBatch::try_new(
            runs_schema(),
            vec![
                Arc::new(StringArray::from(vec![run_id.as_str()])),
                Arc::new(StringArray::from(vec![Some(run.name.as_str())])),
                Arc::new(StringArray::from(vec![Some(run.experiment.as_str())])),
                Arc::new(StringArray::from(vec!["finished"])),
                Arc::new(TimestampMicrosecondArray::from(vec![now]).with_timezone("UTC")),
                Arc::new(Int64Array::from(vec![seq])),
            ],
        )?;
        let mut flat = Vec::new();
        flatten("", &run.params, &mut flat);
        let params = RecordBatch::try_new(
            params_schema(),
            vec![
                Arc::new(StringArray::from(vec![run_id.as_str(); flat.len()])),
                Arc::new(StringArray::from_iter_values(flat.iter().map(|(k, _)| k))),
                Arc::new(StringArray::from_iter_values(
                    flat.iter().map(|(_, v)| v.to_string()),
                )),
                Arc::new(Float64Array::from_iter(
                    flat.iter().map(|(_, v)| v.as_f64()),
                )),
                Arc::new(Int64Array::from(vec![seq; flat.len()])),
            ],
        )?;
        let n: usize = run.series.iter().map(|s| s.steps.len()).sum();
        let mut sid = Vec::with_capacity(n);
        let mut steps = Vec::with_capacity(n);
        let mut values = Vec::with_capacity(n);
        let mut series: Vec<(&str, String, i64)> = Vec::new();
        for s in &run.series {
            let ctx = context_json(s.subset);
            let id = series_id(self.layout, &run_id, s.name, &ctx);
            series.push((s.name, ctx, id));
            sid.extend(std::iter::repeat_n(id, s.steps.len()));
            steps.extend_from_slice(&s.steps);
            values.extend_from_slice(&s.values);
        }
        let mut cols: Vec<Arc<dyn Array>> = Vec::with_capacity(5);
        if self.layout == Layout::Own {
            cols.push(Arc::new(StringArray::from(vec![run_id.as_str(); n])));
        }
        cols.push(Arc::new(Int64Array::from(sid)));
        cols.push(Arc::new(Int64Array::from(steps)));
        if self.layout == Layout::Kv0 {
            cols.push(Arc::new(Int64Array::from(vec![None::<i64>; n])));
        }
        cols.push(Arc::new(
            TimestampMicrosecondArray::from(vec![now; n]).with_timezone("UTC"),
        ));
        cols.push(Arc::new(Float64Array::from(values)));
        let metrics = RecordBatch::try_new(metrics_schema(self.layout), cols)?;
        let series = RecordBatch::try_new(
            series_schema(),
            vec![
                Arc::new(StringArray::from(vec![run_id.as_str(); series.len()])),
                Arc::new(Int64Array::from_iter_values(series.iter().map(|s| s.2))),
                Arc::new(StringArray::from_iter_values(series.iter().map(|s| s.0))),
                Arc::new(StringArray::from_iter_values(
                    series.iter().map(|s| s.1.as_str()),
                )),
                Arc::new(Int64Array::from(vec![seq; series.len()])),
            ],
        )?;
        let calls = t0.elapsed();
        // The flush: four commits at once, one round trip of latency (D18).
        let t1 = Instant::now();
        tokio::try_join!(
            self.runs.append(vec![runs]),
            self.params.append(vec![params]),
            self.series.append(vec![series]),
            self.metrics.append(vec![metrics]),
        )?;
        Ok((calls, t1.elapsed()))
    }

    pub async fn runs(&self) -> Result<Vec<(String, String)>> {
        let b = self
            .runs
            .scan()
            .select(["run_id", "name"])
            .collect()
            .await?;
        let mut ids: Vec<(String, String)> = b
            .iter()
            .flat_map(|b| {
                let a = b.column(0).as_string::<i32>();
                let n = b.column(1).as_string::<i32>();
                (0..a.len())
                    .map(|i| (a.value(i).to_string(), n.value(i).to_string()))
                    .collect::<Vec<_>>()
            })
            .collect();
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    pub async fn run_detail(&self, run_id: &str) -> Result<usize> {
        let f = || col("run_id").eq(lit(run_id));
        let (_, series) = tokio::try_join!(
            self.params.scan().filter(f()).collect(),
            self.series
                .scan()
                .filter(f())
                .select(["series_id"])
                .collect(),
        )?;
        let mut ids: Vec<i64> = series
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect();
        ids.sort_unstable();
        ids.dedup();
        Ok(ids.len())
    }

    pub async fn read_series(
        &self,
        run_id: &str,
        name: &str,
        subset: &str,
        start: Option<i64>,
        stop: Option<i64>,
    ) -> Result<Vec<(i64, f64)>> {
        let id = series_id(self.layout, run_id, name, &context_json(subset));
        let mut s = self.metrics.scan();
        if self.layout == Layout::Own {
            s = s.filter(col("run_id").eq(lit(run_id)));
        }
        s = s
            .filter(col("series_id").eq(lit(id)))
            .select(["step", "value"]);
        if let Some(a) = start {
            s = s.filter(col("step").gt_eq(lit(a)));
        }
        if let Some(b) = stop {
            s = s.filter(col("step").lt(lit(b)));
        }
        let mut out = Vec::new();
        for b in s.collect().await? {
            let st = b.column(0).as_primitive::<Int64Type>();
            let v = b.column(1).as_primitive::<arrow::datatypes::Float64Type>();
            out.extend((0..b.num_rows()).map(|i| (st.value(i), v.value(i))));
        }
        out.sort_by_key(|p| p.0);
        Ok(out)
    }

    async fn records_table(&self, name: &str) -> Result<Table> {
        let mut g = self.records.lock().await;
        if let Some(t) = g.get(name) {
            return Ok(t.clone());
        }
        let t = table(
            &self.db,
            &format!("rec-{name}"),
            records_schema(),
            keyed(&["run_id", "item_id"], &["run_id"]),
        )
        .await?;
        g.insert(name.to_string(), t.clone());
        Ok(t)
    }

    /// Compaction (kv0's `optimize`): every table merged to the most compact layout, then a
    /// checkpoint so that opening reads it.
    pub async fn optimize(&self) -> Result<()> {
        // `MORAINE_BENCH_COMPACTION=memory` measures the in-memory path (every input in
        // memory) in the same build, for the before-and-after of streaming compaction.
        let policy = moraine::CompactionPolicy::full()
            .streaming(std::env::var("MORAINE_BENCH_COMPACTION").as_deref() != Ok("memory"));
        for name in self.db.table_names().await? {
            let t = self.db.open_table(&name).await?;
            t.compact(&policy).await?;
            t.checkpoint().await?;
        }
        Ok(())
    }

    /// Stops the ingest loop (queued mode) and releases its lease.
    pub async fn close(&self) -> Result<()> {
        let mut g = self.ingesting.lock().await;
        if !g.is_empty() {
            for h in g.drain(..) {
                h.abort();
            }
            // Hand the ingest lease on at once: the writers still waiting need an ingestor.
            self.records_table("events").await?.release_leases().await?;
        }
        Ok(())
    }

    /// Queued mode: submits the batch, then waits until an ingestor committed it. The first
    /// call for a table starts this handle's ingest loop.
    async fn write_queued(&self, t: &Table, b: RecordBatch) -> Result<()> {
        {
            let mut g = self.ingesting.lock().await;
            if g.is_empty() {
                let t = t.clone();
                g.push(tokio::spawn(async move {
                    loop {
                        let _ = t.ingest(true).await;
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                }));
            }
        }
        let s = t.submit(vec![b]).await?;
        while !t.is_ingested(&s).await? {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok(())
    }

    /// Appends rows with fresh keys, or upserts (`rewrite`) rows whose keys may exist.
    pub async fn write_records(
        &self,
        table: &str,
        rows: Vec<(String, Option<String>, Value)>,
        rewrite: bool,
    ) -> Result<()> {
        let t = self.records_table(table).await?;
        let n = rows.len();
        let seq: Vec<i64> = (0..n).map(|_| self.next_seq()).collect();
        let b = RecordBatch::try_new(
            records_schema(),
            vec![
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|r| r.1.as_deref().unwrap_or("")),
                )),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| &r.0))),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|r| r.2.to_string()),
                )),
                Arc::new(Int64Array::from(seq)),
            ],
        )?;
        if rewrite {
            t.upsert(vec![b]).await?;
        } else if self.queued {
            self.write_queued(&t, b).await?;
        } else {
            t.append(vec![b]).await?;
        }
        Ok(())
    }

    /// Every stored row of a records table, duplicates of a key included.
    pub async fn raw_records(&self, table: &str) -> Result<usize> {
        Ok(self.records_table(table).await?.scan().count().await?)
    }

    /// Matching rows, every column, data parsed from JSON (a filtered scan, as kv0 reads).
    pub async fn records(
        &self,
        table: &str,
        item_id: Option<&str>,
        run_id: Option<&str>,
        run_ids: Option<&[String]>,
    ) -> Result<Vec<Rec>> {
        let t = self.records_table(table).await?;
        let mut s = t.scan().select(["run_id", "item_id", "data"]);
        if let Some(i) = item_id {
            s = s.filter(col("item_id").eq(lit(i)));
        }
        if let Some(r) = run_id {
            s = s.filter(col("run_id").eq(lit(r)));
        }
        if let Some(rs) = run_ids {
            s = s
                .filter(col("run_id").in_list(rs.iter().map(|r| lit(r.as_str())).collect(), false));
        }
        let mut out = Vec::new();
        for b in s.collect().await? {
            let run = b.column(0).as_string::<i32>();
            let item = b.column(1).as_string::<i32>();
            let data = b.column(2).as_string::<i32>();
            for i in 0..b.num_rows() {
                let r = run.value(i);
                out.push(Rec {
                    item_id: item.value(i).to_string(),
                    run_id: (!r.is_empty()).then(|| r.to_string()),
                    data: serde_json::from_str(data.value(i))?,
                });
            }
        }
        Ok(out)
    }

    /// A record by primary key (run id, item id): moraine's key lookup path.
    pub async fn record_by_key(
        &self,
        table: &str,
        run_id: Option<&str>,
        item_id: &str,
    ) -> Result<Option<Value>> {
        let t = self.records_table(table).await?;
        let key = [
            ScalarValue::Utf8(Some(run_id.unwrap_or("").to_string())),
            ScalarValue::Utf8(Some(item_id.to_string())),
        ];
        match t.get(&key).await? {
            None => Ok(None),
            Some(b) if b.num_rows() == 0 => Ok(None),
            Some(b) => {
                let i = b.schema().index_of("data")?;
                let data = b.column(i).as_string::<i32>();
                Ok(Some(serde_json::from_str(data.value(0))?))
            }
        }
    }
}

fn flatten(prefix: &str, v: &Value, out: &mut Vec<(String, Value)>) {
    match v {
        Value::Object(m) => {
            for (k, x) in m {
                let key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                flatten(&key, x, out);
            }
        }
        other => out.push((prefix.to_string(), other.clone())),
    }
}
