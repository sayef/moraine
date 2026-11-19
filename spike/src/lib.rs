//! Shared helpers for the spike tests. Throwaway code: it exists to answer the twelve
//! questions listed in `docs/SPIKE.md`, and its findings are written up in `docs/SPIKE.md`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use deltalake::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use deltalake::arrow::datatypes::{DataType as ArrowType, Field, Schema as ArrowSchema};
use deltalake::kernel::transaction::CommitProperties;
use deltalake::kernel::{DataType, PrimitiveType, StructField, Transaction};
use deltalake::{DeltaTable, DeltaTableBuilder};
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use url::Url;

pub type Result<T, E = Box<dyn std::error::Error + Send + Sync>> = std::result::Result<T, E>;

/// Environment variables for the S3-compatible test server (see `spike/s3-server.sh`).
pub const ENV_S3_ENDPOINT: &str = "MORAINE_SPIKE_S3_ENDPOINT";
pub const ENV_S3_BUCKET: &str = "MORAINE_SPIKE_S3_BUCKET";
/// Set to a bucket name to run the real-AWS variants. Credentials come from the usual AWS
/// environment (profile, SSO, or instance role); nothing is created by the tests except objects
/// under a random prefix, which they delete again.
pub const ENV_REAL_S3_BUCKET: &str = "MORAINE_SPIKE_REAL_S3_BUCKET";

/// Where a spike table lives. All three share one code path through `object_store`.
#[derive(Clone, Debug)]
pub enum Backend {
    /// One in-memory store shared by every handle in this process.
    Memory(Arc<InMemory>),
    /// A directory on the local filesystem.
    Local(std::path::PathBuf),
    /// An S3 bucket, optionally at a custom endpoint (RustFS in Docker), under a prefix.
    S3 {
        bucket: String,
        prefix: String,
        options: HashMap<String, String>,
    },
}

impl Backend {
    pub fn memory() -> Self {
        Backend::Memory(Arc::new(InMemory::new()))
    }

    /// The S3-compatible server from `spike/s3-server.sh`, if its endpoint variable is set.
    pub fn s3_compatible(prefix: &str) -> Option<Self> {
        let endpoint = std::env::var(ENV_S3_ENDPOINT).ok()?;
        let bucket = std::env::var(ENV_S3_BUCKET).unwrap_or_else(|_| "moraine-spike".into());
        let mut options = HashMap::new();
        options.insert("aws_endpoint".into(), endpoint);
        options.insert("aws_allow_http".into(), "true".into());
        options.insert(
            "aws_region".into(),
            std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into()),
        );
        for (env, key) in [
            ("AWS_ACCESS_KEY_ID", "aws_access_key_id"),
            ("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key"),
        ] {
            if let Ok(v) = std::env::var(env) {
                options.insert(key.into(), v);
            }
        }
        Some(Backend::S3 {
            bucket,
            prefix: prefix.into(),
            options,
        })
    }

    /// Real AWS S3, if `MORAINE_SPIKE_REAL_S3_BUCKET` is set.
    pub fn real_s3(prefix: &str) -> Option<Self> {
        let bucket = std::env::var(ENV_REAL_S3_BUCKET).ok()?;
        let mut options = HashMap::new();
        if let Ok(r) = std::env::var("AWS_REGION") {
            options.insert("aws_region".into(), r);
        }
        Some(Backend::S3 {
            bucket,
            prefix: prefix.into(),
            options,
        })
    }

    /// URL of `rel` (a path such as `db/t/data`) on this backend.
    pub fn url(&self, rel: &str) -> Url {
        match self {
            Backend::Memory(_) => Url::parse(&format!("memory:///{rel}")).unwrap(),
            Backend::Local(dir) => Url::from_directory_path(dir.join(rel)).unwrap(),
            Backend::S3 { bucket, prefix, .. } => {
                Url::parse(&format!("s3://{bucket}/{prefix}/{rel}")).unwrap()
            }
        }
    }

    /// A string form that a child process can parse back with [`Backend::from_spec`].
    pub fn spec(&self) -> String {
        match self {
            Backend::Memory(_) => {
                panic!("the memory backend cannot be shared with another process")
            }
            Backend::Local(dir) => format!("local:{}", dir.display()),
            Backend::S3 { bucket, prefix, .. } => {
                let real = std::env::var(ENV_REAL_S3_BUCKET).ok().as_deref() == Some(bucket);
                format!("{}:{bucket}/{prefix}", if real { "realS3" } else { "s3" })
            }
        }
    }

    pub fn from_spec(spec: &str) -> Self {
        if let Some(dir) = spec.strip_prefix("local:") {
            return Backend::Local(dir.into());
        }
        let (kind, rest) = spec.split_once(':').expect("backend spec");
        let (_bucket, prefix) = rest.split_once('/').expect("bucket/prefix");
        match kind {
            "s3" => Backend::s3_compatible(prefix).expect("S3 endpoint variable not set"),
            "realS3" => Backend::real_s3(prefix).expect("real S3 bucket variable not set"),
            other => panic!("unknown backend {other}"),
        }
    }

    /// The root object store and the path of `rel` inside it.
    pub fn store(&self) -> Result<(Arc<dyn ObjectStore>, Path)> {
        match self {
            Backend::Memory(m) => Ok((m.clone(), Path::from(""))),
            Backend::Local(dir) => {
                std::fs::create_dir_all(dir)?;
                let fs = object_store::local::LocalFileSystem::new_with_prefix(dir)?;
                Ok((Arc::new(fs), Path::from("")))
            }
            Backend::S3 {
                bucket,
                prefix,
                options,
            } => {
                let mut b = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
                for (k, v) in options {
                    b = b.with_config(k.parse()?, v);
                }
                Ok((Arc::new(b.build()?), Path::from(prefix.as_str())))
            }
        }
    }

    /// A table handle for `rel`, loaded if the table exists.
    pub async fn open(&self, rel: &str) -> Result<DeltaTable> {
        let url = self.url(rel);
        let builder = match self {
            Backend::Memory(m) => {
                DeltaTableBuilder::from_url(url.clone())?.with_storage_backend(m.clone(), url)
            }
            Backend::Local(dir) => {
                std::fs::create_dir_all(dir.join(rel))?;
                DeltaTableBuilder::from_url(url)?
            }
            Backend::S3 { options, .. } => {
                DeltaTableBuilder::from_url(url)?.with_storage_options(options.clone())
            }
        };
        let mut table = builder.build()?;
        match table.load().await {
            Ok(()) | Err(deltalake::DeltaTableError::NotATable(_)) => Ok(table),
            Err(e) => Err(e.into()),
        }
    }

    /// Delete everything under the backend's prefix. Used to clean up after S3 runs.
    pub async fn clear(&self) -> Result<usize> {
        let (store, root) = self.store()?;
        let metas: Vec<_> = store.list(Some(&root)).try_collect().await?;
        for m in &metas {
            store.delete(&m.location).await?;
        }
        Ok(metas.len())
    }
}

/// Schema used by the append and upsert spikes: `writer`, `seq`, `key`, `value`.
pub fn spike_columns() -> Vec<StructField> {
    vec![
        StructField::new("writer", DataType::Primitive(PrimitiveType::Long), false),
        StructField::new("seq", DataType::Primitive(PrimitiveType::Long), false),
        StructField::new("key", DataType::Primitive(PrimitiveType::String), false),
        StructField::new("value", DataType::Primitive(PrimitiveType::Long), false),
    ]
}

pub fn spike_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("writer", ArrowType::Int64, false),
        Field::new("seq", ArrowType::Int64, false),
        Field::new("key", ArrowType::Utf8, false),
        Field::new("value", ArrowType::Int64, false),
    ]))
}

/// A batch of `rows` rows for (`writer`, `seq`), with keys `w<writer>-s<seq>-r<i>`.
pub fn spike_batch(writer: i64, seq: i64, rows: usize, value: i64) -> RecordBatch {
    let keys: Vec<String> = (0..rows)
        .map(|i| format!("w{writer}-s{seq}-r{i}"))
        .collect();
    RecordBatch::try_new(
        spike_arrow_schema(),
        vec![
            Arc::new(Int64Array::from(vec![writer; rows])),
            Arc::new(Int64Array::from(vec![seq; rows])),
            Arc::new(StringArray::from(keys)),
            Arc::new(Int64Array::from(vec![value; rows])),
        ],
    )
    .unwrap()
}

/// A one-row batch for an explicit key.
pub fn key_batch(writer: i64, seq: i64, key: &str, value: i64) -> RecordBatch {
    RecordBatch::try_new(
        spike_arrow_schema(),
        vec![
            Arc::new(Int64Array::from(vec![writer])),
            Arc::new(Int64Array::from(vec![seq])),
            Arc::new(StringArray::from(vec![key.to_string()])),
            Arc::new(Int64Array::from(vec![value])),
        ],
    )
    .unwrap()
}

/// Create the spike table at `rel` with the given configuration. Treats "already exists" as
/// success so several processes can race on it.
pub async fn create_spike_table(
    backend: &Backend,
    rel: &str,
    config: &[(&str, &str)],
) -> Result<DeltaTable> {
    let table = backend.open(rel).await?;
    if table.version().is_some() {
        return Ok(table);
    }
    let cfg: HashMap<String, Option<String>> = config
        .iter()
        .map(|(k, v)| (k.to_string(), Some(v.to_string())))
        .collect();
    let created = table
        .create()
        .with_columns(spike_columns())
        .with_configuration(cfg)
        .with_raise_if_key_not_exists(false)
        .await;
    match created {
        Ok(t) => Ok(t),
        Err(_) => {
            // Lost the race: another client created version 0.
            let t = backend.open(rel).await?;
            assert!(
                t.version().is_some(),
                "table creation failed and no table exists"
            );
            Ok(t)
        }
    }
}

/// Per-writer result of an append run.
#[derive(Debug, Default, Clone)]
pub struct AppendStats {
    pub commits: u64,
    pub outer_retries: u64,
    pub latencies_ms: Vec<f64>,
    pub elapsed_s: f64,
}

impl AppendStats {
    pub fn to_line(&self, writer: i64) -> String {
        let mut l = self.latencies_ms.clone();
        l.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let pct = |p: f64| {
            l.get(((l.len() as f64 - 1.0) * p).round() as usize)
                .copied()
                .unwrap_or(0.0)
        };
        format!(
            "writer={writer} commits={} outer_retries={} elapsed_s={:.2} p50_ms={:.1} p99_ms={:.1} max_ms={:.1}",
            self.commits,
            self.outer_retries,
            self.elapsed_s,
            pct(0.5),
            pct(0.99),
            pct(1.0)
        )
    }
}

/// Append `count` batches of `rows` rows as `writer`. Each commit carries a `txn` action with
/// app id `spike-writer-<writer>` and version `seq`, so a later check can find it.
pub async fn run_appender(
    backend: &Backend,
    rel: &str,
    writer: i64,
    count: i64,
    rows: usize,
) -> Result<AppendStats> {
    let mut stats = AppendStats::default();
    let start = Instant::now();
    let mut table = backend.open(rel).await?;
    let app_id = format!("spike-writer-{writer}");
    for seq in 0..count {
        let t0 = Instant::now();
        loop {
            let props = CommitProperties::default()
                .with_max_retries(10_000)
                .with_application_transaction(Transaction::new(app_id.clone(), seq));
            let res = table
                .clone()
                .write(vec![spike_batch(writer, seq, rows, seq)])
                .with_commit_properties(props)
                .await;
            match res {
                Ok(t) => {
                    table = t;
                    break;
                }
                Err(e) => {
                    // Anything but a conflict is unexpected for blind appends; retry a few times
                    // so a transient store error does not end the run, and report it.
                    stats.outer_retries += 1;
                    eprintln!("writer {writer} seq {seq}: {e}");
                    if stats.outer_retries > 50 {
                        return Err(e.into());
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    table.update_state().await?;
                    // delta-rs reports an error from its post-commit hook (checkpoint, log
                    // cleanup, reloading the state) after the commit has landed; another
                    // process's log cleanup can make it fail. Writing again would append the
                    // batch twice, so look the transaction up first.
                    let applied = table
                        .snapshot()?
                        .transaction_version(table.log_store().as_ref(), app_id.clone())
                        .await?;
                    if applied.is_some_and(|v| v >= seq) {
                        break;
                    }
                }
            }
        }
        stats.commits += 1;
        stats.latencies_ms.push(t0.elapsed().as_secs_f64() * 1e3);
    }
    stats.elapsed_s = start.elapsed().as_secs_f64();
    Ok(stats)
}

/// Read every row of the table with a fresh handle.
pub async fn scan_all(backend: &Backend, rel: &str) -> Result<Vec<RecordBatch>> {
    let table = backend.open(rel).await?;
    let (_, stream) = table.scan_table().await?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    Ok(batches.into_iter().filter(|b| b.num_rows() > 0).collect())
}

/// Count rows per `(writer, seq)` and per key.
pub fn tally(batches: &[RecordBatch]) -> (BTreeMap<(i64, i64), usize>, BTreeMap<String, usize>) {
    let mut by_ws = BTreeMap::new();
    let mut by_key = BTreeMap::new();
    for b in batches {
        assert!(
            b.column_by_name("writer").is_some(),
            "unexpected scan schema: {:?}",
            b.schema()
        );
        let w = b
            .column_by_name("writer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let s = b
            .column_by_name("seq")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // Scans may return `Utf8View`; normalise to `Utf8`.
        let k = deltalake::arrow::compute::cast(b.column_by_name("key").unwrap(), &ArrowType::Utf8)
            .unwrap();
        let k = k.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..b.num_rows() {
            *by_ws.entry((w.value(i), s.value(i))).or_insert(0) += 1;
            if !k.is_null(i) {
                *by_key.entry(k.value(i).to_string()).or_insert(0) += 1;
            }
        }
    }
    (by_ws, by_key)
}

/// List `_delta_log` of the table at `rel`: (commit versions, checkpoint versions, other names).
pub async fn list_log(backend: &Backend, rel: &str) -> Result<(Vec<u64>, Vec<u64>, Vec<String>)> {
    let (store, root) = backend.store()?;
    let prefix = Path::from(format!("{}/_delta_log", join(&root, rel)));
    let metas: Vec<_> = store.list(Some(&prefix)).try_collect().await?;
    let mut commits = vec![];
    let mut checkpoints = vec![];
    let mut other = vec![];
    for m in metas {
        let name = m.location.filename().unwrap_or_default().to_string();
        if let Some(v) = name
            .strip_suffix(".json")
            .and_then(|s| s.parse::<u64>().ok())
        {
            commits.push(v);
        } else if let Some(v) = name
            .strip_suffix(".checkpoint.parquet")
            .and_then(|s| s.parse::<u64>().ok())
        {
            checkpoints.push(v);
        } else {
            other.push(name);
        }
    }
    commits.sort();
    checkpoints.sort();
    other.sort();
    Ok((commits, checkpoints, other))
}

/// Join a store root and a relative path.
pub fn join(root: &Path, rel: &str) -> String {
    if root.as_ref().is_empty() {
        rel.to_string()
    } else {
        format!("{}/{rel}", root.as_ref())
    }
}

/// Read commit file `version` of the table at `rel` as JSON lines.
pub async fn read_commit(
    backend: &Backend,
    rel: &str,
    version: u64,
) -> Result<Vec<serde_json::Value>> {
    let (store, root) = backend.store()?;
    let path = Path::from(format!(
        "{}/_delta_log/{version:020}.json",
        join(&root, rel)
    ));
    let bytes = store.get(&path).await?.bytes().await?;
    Ok(std::str::from_utf8(&bytes)?
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

/// A random prefix, so runs against a shared bucket never collide.
pub fn run_prefix(name: &str) -> String {
    format!("spike-{name}-{}", uuid::Uuid::new_v4().simple())
}

/// Median and 99th percentile of a list of milliseconds.
pub fn percentiles(v: &[f64]) -> (f64, f64, f64) {
    let mut l = v.to_vec();
    l.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |p: f64| {
        l.get(((l.len() as f64 - 1.0) * p).round() as usize)
            .copied()
            .unwrap_or(0.0)
    };
    (pct(0.5), pct(0.99), pct(1.0))
}

/// Run `writers` appender processes (binary at `exe`) against one table and collect their
/// `STATS` lines. Returns the lines and the wall time in seconds.
pub async fn run_appender_processes(
    exe: &str,
    backend: &Backend,
    rel: &str,
    writers: i64,
    count: i64,
    rows: usize,
) -> Result<(Vec<String>, f64)> {
    let start = Instant::now();
    let mut children = vec![];
    for w in 0..writers {
        children.push(
            tokio::process::Command::new(exe)
                .args([
                    backend.spec(),
                    rel.to_string(),
                    w.to_string(),
                    count.to_string(),
                    rows.to_string(),
                ])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()?,
        );
    }
    let mut lines = vec![];
    for c in children {
        let out = c.wait_with_output().await?;
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        if !out.status.success() {
            return Err(format!("appender failed: {stderr}").into());
        }
        let errors = stderr.lines().count();
        for l in stdout.lines().filter(|l| l.starts_with("STATS ")) {
            lines.push(format!("{} stderr_lines={errors}", &l[6..]));
        }
    }
    Ok((lines, start.elapsed().as_secs_f64()))
}

/// Run `writers` appenders as tasks in this process, each with its own table handle.
pub async fn run_appender_tasks(
    backend: &Backend,
    rel: &str,
    writers: i64,
    count: i64,
    rows: usize,
) -> Result<(Vec<String>, f64)> {
    let start = Instant::now();
    let mut tasks = vec![];
    for w in 0..writers {
        let backend = backend.clone();
        let rel = rel.to_string();
        tasks.push(tokio::spawn(async move {
            run_appender(&backend, &rel, w, count, rows)
                .await
                .map(|s| s.to_line(w))
        }));
    }
    let mut lines = vec![];
    for t in tasks {
        lines.push(t.await??);
    }
    Ok((lines, start.elapsed().as_secs_f64()))
}

/// Summary of a finished append run.
#[derive(Debug)]
pub struct AppendCheck {
    pub rows: usize,
    pub expected_rows: usize,
    pub latest_version: u64,
    pub log_versions_present: usize,
    pub gapless: bool,
    pub bad_groups: usize,
    pub txn_ok: bool,
    pub total_retries: u64,
    pub duplicate_txn_commits: usize,
}

/// Verify an append run: every `(writer, seq)` has exactly `rows` rows, the log has every
/// version from 0 to the latest, each writer's last `txn` version is `count - 1`, and no
/// `txn` appears in two commits.
pub async fn check_appends(
    backend: &Backend,
    rel: &str,
    writers: i64,
    count: i64,
    rows: usize,
    first_version: u64,
) -> Result<AppendCheck> {
    let batches = scan_all(backend, rel).await?;
    let (by_ws, _) = tally(&batches);
    let total: usize = by_ws.values().sum();
    let mut bad = 0;
    for w in 0..writers {
        for s in 0..count {
            if by_ws.get(&(w, s)).copied().unwrap_or(0) != rows {
                bad += 1;
            }
        }
    }
    let table = backend.open(rel).await?;
    let latest = table.version().unwrap() as u64;
    let mut txn_ok = true;
    for w in 0..writers {
        let v = table
            .snapshot()?
            .transaction_version(table.log_store().as_ref(), format!("spike-writer-{w}"))
            .await?;
        txn_ok &= v == Some(count - 1);
    }
    let (commits, _, _) = list_log(backend, rel).await?;
    let gapless =
        commits.windows(2).all(|w| w[1] == w[0] + 1) && commits.last().copied() == Some(latest);
    let mut retries = 0;
    let mut seen = std::collections::HashSet::new();
    let mut dup = 0;
    for v in commits.iter().copied().filter(|v| *v >= first_version) {
        for action in read_commit(backend, rel, v).await? {
            if let Some(n) = action
                .pointer("/commitInfo/operationMetrics/numRetries")
                .and_then(|n| {
                    n.as_u64()
                        .or_else(|| n.as_str().and_then(|s| s.parse().ok()))
                })
            {
                retries += n;
            }
            if let Some(t) = action.get("txn")
                && !seen.insert((t["appId"].to_string(), t["version"].to_string()))
            {
                dup += 1;
            }
        }
    }
    Ok(AppendCheck {
        rows: total,
        expected_rows: writers as usize * count as usize * rows,
        latest_version: latest,
        log_versions_present: commits.len(),
        gapless,
        bad_groups: bad,
        txn_ok,
        total_retries: retries,
        duplicate_txn_commits: dup,
    })
}

/// An `ObjectStore` wrapper that counts GET requests and the bytes they return.
#[derive(Debug)]
pub struct CountingStore {
    pub inner: Arc<dyn ObjectStore>,
    pub gets: std::sync::atomic::AtomicU64,
    pub bytes: std::sync::atomic::AtomicU64,
}

impl CountingStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            gets: 0.into(),
            bytes: 0.into(),
        }
    }
    pub fn take(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering::SeqCst;
        (self.gets.swap(0, SeqCst), self.bytes.swap(0, SeqCst))
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Counting({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        use std::sync::atomic::Ordering::SeqCst;
        let head = options.head;
        let r = self.inner.get_opts(location, options).await?;
        self.gets.fetch_add(1, SeqCst);
        if !head {
            self.bytes.fetch_add(r.range.end - r.range.start, SeqCst);
        }
        Ok(r)
    }
    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        use std::sync::atomic::Ordering::SeqCst;
        let r = self.inner.get_ranges(location, ranges).await?;
        self.gets.fetch_add(ranges.len() as u64, SeqCst);
        self.bytes
            .fetch_add(r.iter().map(|b| b.len() as u64).sum(), SeqCst);
        Ok(r)
    }
    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
