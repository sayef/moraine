//! The database handle: where tables live, the store stack, the clock, the
//! shared caches, and table creation and opening.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::DFSchema;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;
use futures::FutureExt;
use object_store::ObjectStore;
use object_store::path::Path;
use url::Url;

use crate::clock::{Clock, SystemClock, random_name};
use crate::commit::{AttemptError, ConflictKind, commit_with_retries};
use crate::error::{Error, Result};
use crate::log::delta::DeltaLog;
use crate::log::{CommitLayer, LogEntry, NewEntry, TableMeta};
use crate::names::NotFound;
use crate::options::{Consistency, TableDef, TableOptions};
use crate::read::FileCache;
use crate::schema::{column_not_found, validate_new_schema, validate_table_name};
use crate::snapshot::Snapshot;
use crate::store::{LocalCasStore, RetryPolicy, Store, StoreMetrics, probe_conditional_writes};
use crate::table::Table;

/// What the caches of a [`Database`] hold (see [`Database::cache_usage`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CacheUsage {
    /// Parquet footers and page indexes, and decoded key indexes (D20):
    /// [`DatabaseBuilder::file_cache_bytes`].
    pub files: usize,
    /// Bytes of those entries.
    pub file_bytes: usize,
    /// Sidecar bundles stored on disk and held in memory: the sidecar cache's `max_bytes`.
    pub sidecar_disk_bytes: u64,
    /// Bytes of sidecar bundles held in memory.
    pub sidecar_memory_bytes: u64,
    /// Opened sidecars and the heap they report: [`DatabaseBuilder::max_open_sidecars`].
    pub open_sidecars: usize,
    /// Heap bytes the opened sidecars report.
    pub open_sidecar_bytes: usize,
    /// Full-text indexes built in memory for files without a sidecar:
    /// [`DatabaseBuilder::fts_memory`].
    pub fts_indexes: usize,
    /// Bytes of those indexes.
    pub fts_bytes: usize,
    /// Filter bitmaps of searches (at most 4,096).
    pub filter_bitmaps: usize,
    /// Small data pages kept from reads by row offset (search hits), at most an eighth of the
    /// file cache's budget.
    pub page_bytes: usize,
}

/// Settings of a database handle.
#[derive(Clone, Debug)]
pub(crate) struct DbOptions {
    pub consistency: Consistency,
    /// Parallel requests per operation (file reads, log reads). The simulation uses 1.
    pub io_concurrency: usize,
    pub file_cache_bytes: usize,
    pub small_table_bytes: u64,
    pub small_table_rows: u64,
    pub linger: Duration,
    pub max_query_duration: Duration,
    pub commit_policy: RetryPolicy,
    /// What writes do with blob references they did not upload.
    pub blob_reuse: crate::blob::BlobReuse,
}

impl Default for DbOptions {
    fn default() -> Self {
        DbOptions {
            consistency: Consistency::default(),
            io_concurrency: 16,
            file_cache_bytes: 512 << 20,
            small_table_bytes: 64 << 20,
            small_table_rows: 250_000,
            linger: Duration::ZERO,
            max_query_duration: Duration::from_secs(300),
            commit_policy: RetryPolicy::COMMIT,
            blob_reuse: crate::blob::BlobReuse::Copy,
        }
    }
}

/// Everything the tables of one database share.
pub(crate) struct DbShared {
    pub store: Store,
    pub root: Path,
    pub url: Url,
    pub writer_id: String,
    txn_seq: AtomicU64,
    pub cache: FileCache,
    pub opts: DbOptions,
    writable: Mutex<Option<std::result::Result<(), String>>>,
    session: SessionContext,
    /// Index kinds, sidecar cache and builder state (D33).
    pub index: crate::index::IndexRuntime,
}

impl std::fmt::Debug for DbShared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbShared")
            .field("root", &self.root)
            .field("writer_id", &self.writer_id)
            .finish_non_exhaustive()
    }
}

impl DbShared {
    pub fn clock(&self) -> &Arc<dyn Clock> {
        self.store.clock()
    }

    pub fn next_txn(&self) -> i64 {
        self.txn_seq.fetch_add(1, Ordering::Relaxed) as i64 + 1
    }

    /// Writes need a store that honours conditional writes (case S2). Probed once per handle.
    pub async fn require_writable(&self) -> Result<()> {
        if let Some(r) = self.writable.lock().ok().and_then(|g| g.clone()) {
            return r.map_err(Error::ConditionalWritesUnsupported);
        }
        let prefix = self.root.clone().join("_meta");
        let caps = probe_conditional_writes(
            self.store.object_store().as_ref(),
            &prefix,
            self.clock().as_ref(),
        )
        .await?;
        let r = if caps.can_write() {
            Ok(())
        } else {
            Err(format!(
                "put-if-absent {:?}, compare-and-swap {:?}",
                caps.create, caps.update
            ))
        };
        if let Ok(mut g) = self.writable.lock() {
            *g = Some(r.clone());
        }
        r.map_err(Error::ConditionalWritesUnsupported)
    }

    /// A filter compiled for batches of `schema`, with DataFusion's type coercion.
    pub fn physical_expr(&self, e: Expr, schema: &SchemaRef) -> Result<Arc<dyn PhysicalExpr>> {
        for c in e.column_refs() {
            if schema.field_with_name(&c.name).is_err() {
                return Err(column_not_found(&c.name, schema));
            }
        }
        let df = DFSchema::try_from(schema.as_ref().clone())?;
        self.session
            .create_physical_expr(e, &df)
            .map_err(|e| Error::InvalidQuery(e.to_string()))
    }

    fn layer(&self, name: &str) -> Arc<dyn CommitLayer> {
        let root = self.root.clone().join(name).join("data");
        let mut url = self.url.clone();
        let base = url.path().trim_end_matches('/').to_string();
        url.set_path(&format!("{base}/{name}/data"));
        Arc::new(DeltaLog::new(
            self.store.object_store().clone(),
            root,
            url,
            self.opts.io_concurrency,
        ))
    }

    /// The grace-period inequality (case G6).
    fn check_grace(&self, grace: Duration) -> Result<()> {
        let staleness = self.opts.consistency.max_staleness();
        let need = self.opts.max_query_duration + staleness + grace / 4;
        if grace < need {
            return Err(Error::InvalidConfig(format!(
                "grace period {grace:?} is shorter than max query duration + staleness + commit \
                 deadline ({need:?})"
            )));
        }
        Ok(())
    }
}

/// A database: a prefix of an object store holding tables. Cheap to clone.
#[derive(Clone, Debug)]
pub struct Database {
    shared: Arc<DbShared>,
}

/// Builds a [`Database`].
#[derive(Clone, Debug)]
pub struct DatabaseBuilder {
    location: Location,
    storage_options: HashMap<String, String>,
    clock: Option<Arc<dyn Clock>>,
    request_policy: RetryPolicy,
    opts: DbOptions,
    tags: Vec<(String, String)>,
    pub(crate) index: crate::index::IndexConfig,
}

#[derive(Clone, Debug)]
enum Location {
    Url(String),
    Store {
        store: Arc<dyn ObjectStore>,
        prefix: Path,
    },
}

impl Database {
    /// A database at `location`: `s3://bucket/prefix`, `file:///dir`, a local directory path,
    /// or `memory://` (one process only).
    pub fn builder(location: &str) -> DatabaseBuilder {
        DatabaseBuilder::new(Location::Url(location.to_string()))
    }

    /// A database under `prefix` of an existing store (tests and embedding).
    pub fn builder_for_store(store: Arc<dyn ObjectStore>, prefix: &str) -> DatabaseBuilder {
        DatabaseBuilder::new(Location::Store {
            store,
            prefix: Path::from(prefix),
        })
    }

    /// Counters of the index machinery (sidecar and exact searches, bad sidecars).
    pub fn index_metrics(&self) -> &Arc<crate::index::IndexMetrics> {
        &self.shared.index.metrics
    }

    /// What the database's caches hold now; each stays within its configured limit.
    pub fn cache_usage(&self) -> CacheUsage {
        let i = &self.shared.index;
        let (files, file_bytes) = self.shared.cache.usage();
        let (disk, mem) = i.cache.bytes();
        let (open_sidecars, open_sidecar_bytes) = i.handles.usage();
        let (fts_indexes, fts_bytes) = i.fallback.usage();
        CacheUsage {
            files,
            file_bytes,
            sidecar_disk_bytes: disk,
            sidecar_memory_bytes: mem,
            open_sidecars,
            open_sidecar_bytes,
            fts_indexes,
            fts_bytes,
            filter_bitmaps: i.bitmaps(),
            page_bytes: self.shared.cache.pages.bytes(),
        }
    }

    /// Request and byte counters of everything this handle sent to the store.
    pub fn store_metrics(&self) -> &Arc<StoreMetrics> {
        self.shared.store.metrics()
    }

    /// Creates table `name`. When it exists already with the same schema and options, opens it;
    /// otherwise [`Error::TableExistsWithDifferentSchema`] (case C12).
    pub async fn create_table(
        &self,
        name: &str,
        schema: SchemaRef,
        options: TableOptions,
    ) -> Result<Table> {
        validate_table_name(name)?;
        let props = options.to_properties()?;
        let def = TableDef::from_properties(&props)?;
        self.shared.check_grace(def.grace_period)?;
        let layer = self.shared.layer(name);
        let canonical = layer.canonical_schema(&schema)?;
        validate_new_schema(&canonical, &def)?;
        self.shared.require_writable().await?;
        let clock = self.shared.clock().clone();
        let meta = TableMeta {
            id: uuid_like(clock.as_ref()),
            schema: canonical.clone(),
            partition_columns: Vec::new(),
            configuration: props,
            protocol: None,
        };
        let body = layer.encode(
            &NewEntry {
                meta: Some(meta.clone()),
                protocol: true,
                adds: Vec::new(),
                removes: Vec::new(),
                txn: None,
                caller_txns: Vec::new(),
                operation: "CREATE TABLE",
                timestamp: clock.wall_now().timestamp_millis(),
                data_change: true,
            },
            &canonical,
        )?;
        fail::fail_point!("moraine::create::before_commit");
        let (l1, l2, b, id) = (layer.clone(), layer.clone(), body.clone(), meta.id.clone());
        let (created, _) = commit_with_retries(
            &self.shared.opts.commit_policy,
            clock.as_ref(),
            &meta.id,
            move |_| {
                let (l, b) = (l1.clone(), b.clone());
                async move {
                    match l.put_entry(0, b).await {
                        Ok(()) => Ok(true),
                        Err(AttemptError::Conflict(ConflictKind::VersionTaken)) => Ok(false),
                        Err(e) => Err(e),
                    }
                }
                .boxed()
            },
            move || {
                let (l, id) = (l2.clone(), id.clone());
                async move {
                    Ok(l.read_entry(0, None)
                        .await?
                        .map(|e| e.meta.is_some_and(|m| m.id == id)))
                }
                .boxed()
            },
        )
        .await;
        if created? {
            let entry = LogEntry {
                version: 0,
                meta: Some(Arc::new(meta)),
                ..Default::default()
            };
            let snap = Snapshot::from_loaded(
                crate::log::Loaded {
                    base: None,
                    base_txns: Vec::new(),
                    base_txn_times: Vec::new(),
                    entries: vec![entry],
                },
                clock.now(),
            )?;
            return Ok(Table::new(name.into(), self.shared.clone(), layer, snap));
        }
        let t = self.open_table(name).await?;
        let theirs = t.definition();
        if !same_schema(&t.schema(), &canonical) || theirs != def {
            return Err(Error::TableExistsWithDifferentSchema(name.into()));
        }
        Ok(t)
    }

    /// Opens table `name`: one listing of its log and a read of the entries since the last
    /// checkpoint. [`Error::TableNotFound`] when it has no log.
    pub async fn open_table(&self, name: &str) -> Result<Table> {
        validate_table_name(name)?;
        let layer = self.shared.layer(name);
        let loaded = match layer.load().await {
            Err(Error::TableNotFound(_)) => return Err(self.table_not_found(name).await),
            other => other?,
        };
        let snap = Snapshot::from_loaded(loaded, self.shared.clock().now())?;
        self.shared.check_grace(snap.def.grace_period)?;
        Ok(Table::new(name.into(), self.shared.clone(), layer, snap))
    }
}

impl Database {
    /// The names of the tables in the database, sorted: the valid table names among the
    /// prefixes under the root (one listing).
    pub async fn table_names(&self) -> Result<Vec<String>> {
        let l = self
            .shared
            .store
            .object_store()
            .list_with_delimiter(Some(&self.shared.root))
            .await?;
        let mut names: Vec<String> = l
            .common_prefixes
            .into_iter()
            .filter_map(|p| p.filename().map(str::to_owned))
            .filter(|n| validate_table_name(n).is_ok())
            .collect();
        names.sort();
        Ok(names)
    }

    /// [`Error::TableNotFound`] for `name`, with the tables that exist. Costs one listing, on
    /// the error path only; a failed listing leaves the names out.
    async fn table_not_found(&self, name: &str) -> Error {
        let listed = self
            .shared
            .store
            .object_store()
            .list_with_delimiter(Some(&self.shared.root))
            .await;
        let names = listed.map(|l| {
            l.common_prefixes
                .into_iter()
                .filter_map(|p| p.filename().map(str::to_owned))
                .filter(|n| validate_table_name(n).is_ok())
                .collect::<Vec<_>>()
        });
        Error::TableNotFound(match names {
            Ok(names) => NotFound::among(name, names),
            Err(_) => NotFound::bare(name),
        })
    }
}

fn same_schema(a: &Schema, b: &Schema) -> bool {
    a.fields().len() == b.fields().len()
        && a.fields().iter().zip(b.fields()).all(|(x, y)| {
            x.name() == y.name()
                && x.data_type() == y.data_type()
                && x.is_nullable() == y.is_nullable()
        })
}

fn uuid_like(clock: &dyn Clock) -> String {
    let h = random_name(clock);
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

impl DatabaseBuilder {
    fn new(location: Location) -> Self {
        DatabaseBuilder {
            location,
            storage_options: HashMap::new(),
            clock: None,
            request_policy: RetryPolicy::default(),
            opts: DbOptions::default(),
            tags: Vec::new(),
            index: crate::index::IndexConfig::default(),
        }
    }

    /// Registers an index kind (D24, D29: kinds are pluggable from outside the crate). A kind
    /// with the name of a built-in one replaces it. Every client of a table must register the
    /// kinds its indexes use; a client without one keeps the index but cannot search it.
    pub fn index_kind(mut self, kind: Arc<dyn crate::index::IndexKind>) -> Self {
        self.index.kinds.push(kind);
        self
    }

    /// Options passed to the object store (for S3: `aws_endpoint`, `aws_region`, ...).
    ///
    /// With the `aws-sdk` feature, an `s3://` database without keys takes credentials from the
    /// AWS SDK's default chain (`aws_profile` names the profile; otherwise `AWS_PROFILE`), and
    /// one without a region or endpoint uses the bucket's own region.
    pub fn storage_options(mut self, o: HashMap<String, String>) -> Self {
        self.storage_options = o;
        self
    }

    /// The clock for timing, backoff and random names (default [`SystemClock`]). The test
    /// kit's simulation passes its own to control time.
    pub fn clock(mut self, c: Arc<dyn Clock>) -> Self {
        self.clock = Some(c);
        self
    }

    /// When handles look for other clients' commits (default
    /// [`Consistency::Eventual`] with 5 seconds); each [`Table`] can change its own with
    /// [`Table::set_consistency`].
    pub fn consistency(mut self, c: Consistency) -> Self {
        self.opts.consistency = c;
        self
    }

    /// Parallel requests per operation (default 16).
    pub fn io_concurrency(mut self, n: usize) -> Self {
        self.opts.io_concurrency = n.max(1);
        self
    }

    /// Bytes of Parquet footers, page indexes and small files kept in memory (default 512 MiB).
    pub fn file_cache_bytes(mut self, n: usize) -> Self {
        self.opts.file_cache_bytes = n;
        self
    }

    /// Tables whose live data is at most this size are read once per version and filtered in
    /// memory (D20; default 64 MiB, 0 turns it off).
    pub fn small_table_bytes(mut self, n: u64) -> Self {
        self.opts.small_table_bytes = n;
        self
    }

    /// ... and at most this many rows (default 250,000): Parquet compresses well, so bytes
    /// alone would admit tables whose in-memory filtering costs more than reading pages.
    pub fn small_table_rows(mut self, n: u64) -> Self {
        self.opts.small_table_rows = n;
        self
    }

    /// What writes do with a blob reference they did not upload themselves: copy it on the
    /// server to a fresh key (the default), or trust the caller that it was referenced without
    /// a gap.
    pub fn blob_reuse(mut self, r: crate::blob::BlobReuse) -> Self {
        self.opts.blob_reuse = r;
        self
    }

    /// How long the commit task waits for more writes before taking a group (default 0).
    pub fn group_commit_linger(mut self, d: Duration) -> Self {
        self.opts.linger = d;
        self
    }

    /// The longest a query may run against one snapshot (default 5 minutes). Opening checks
    /// each table's grace period against it: the grace period must cover this duration, the
    /// consistency's staleness and the commit deadline (case G6), or
    /// [`Error::InvalidConfig`] is returned.
    pub fn max_query_duration(mut self, d: Duration) -> Self {
        self.opts.max_query_duration = d;
        self
    }

    /// Tags added to every object this handle writes (data files, log entries, probes), for
    /// cost allocation and lifecycle rules. They ride on the write request, so they cost no
    /// request. At most 10 pairs; keys of 1 to 128 and values of at most 256 characters (S3's
    /// limits). Stores without tags ignore them.
    pub fn object_tags<I, K, V>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.tags = tags
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        self
    }

    /// Retries of single requests.
    pub fn request_retry(mut self, p: RetryPolicy) -> Self {
        self.request_policy = p;
        self
    }

    /// Retries of commits that conflicted or failed transiently.
    pub fn commit_retry(mut self, p: RetryPolicy) -> Self {
        self.opts.commit_policy = p;
        self
    }

    /// Opens the database: connects the store and checks the configuration, without a request.
    /// The first write probes the store for conditional writes once (case S2); a store that
    /// fails the probe still reads, and its writes return
    /// [`Error::ConditionalWritesUnsupported`].
    pub async fn open(self) -> Result<Database> {
        let clock: Arc<dyn Clock> = self.clock.unwrap_or_else(|| Arc::new(SystemClock::new()));
        let (backend, root, url) = match self.location {
            Location::Store { store, prefix } => {
                let url = Url::parse(&format!("memory:///{prefix}"))
                    .map_err(|e| Error::InvalidConfig(e.to_string()))?;
                (store, prefix, url)
            }
            Location::Url(u) => open_url(&u, &self.storage_options).await?,
        };
        check_tags(&self.tags)?;
        let store = Store::with_tags(backend, clock.clone(), self.request_policy, self.tags);
        let writer_id = random_name(clock.as_ref());
        let index = crate::index::IndexRuntime::new(self.index)?;
        Ok(Database {
            shared: Arc::new(DbShared {
                store,
                root,
                url,
                writer_id,
                txn_seq: AtomicU64::new(0),
                cache: FileCache::new(self.opts.file_cache_bytes),
                opts: self.opts,
                writable: Mutex::new(None),
                session: SessionContext::new(),
                index,
            }),
        })
    }
}

/// A storage option moraine reads itself: the shared-config profile for the AWS SDK's
/// credential chain (feature `aws-sdk`).
const AWS_PROFILE: &str = "aws_profile";

/// S3's limits on object tags, the strictest of the supported stores.
fn check_tags(tags: &[(String, String)]) -> Result<()> {
    let bad = |m: String| Err(Error::InvalidConfig(format!("object tags: {m}")));
    if tags.len() > 10 {
        return bad(format!("{} tags, at most 10 allowed", tags.len()));
    }
    let mut seen = std::collections::HashSet::new();
    for (k, v) in tags {
        if k.is_empty() || k.chars().count() > 128 {
            return bad(format!("key {k:?} must have 1 to 128 characters"));
        }
        if v.chars().count() > 256 {
            return bad(format!("value of {k:?} has more than 256 characters"));
        }
        if !seen.insert(k) {
            return bad(format!("key {k:?} given twice"));
        }
    }
    Ok(())
}

async fn open_url(
    u: &str,
    options: &HashMap<String, String>,
) -> Result<(Arc<dyn ObjectStore>, Path, Url)> {
    let bad = |e: String| Error::InvalidConfig(format!("{u}: {e}"));
    if u.starts_with("memory://") {
        let url = Url::parse("memory:///").map_err(|e| bad(e.to_string()))?;
        return Ok((
            Arc::new(object_store::memory::InMemory::new()),
            Path::from(""),
            url,
        ));
    }
    if let Some(rest) = u.strip_prefix("s3://") {
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let mut b = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
        let mut has_cas = false;
        let profile = options.get(AWS_PROFILE).cloned();
        for (k, v) in options.iter().filter(|(k, _)| k.as_str() != AWS_PROFILE) {
            let key: object_store::aws::AmazonS3ConfigKey = k
                .parse()
                .map_err(|e: object_store::Error| bad(e.to_string()))?;
            has_cas |= matches!(key, object_store::aws::AmazonS3ConfigKey::ConditionalPut);
            b = b.with_config(key, v);
        }
        if !has_cas {
            // Compare-and-swap (`If-Match`) for leases; put-if-absent needs no setting.
            b = b.with_config(object_store::aws::AmazonS3ConfigKey::ConditionalPut, "etag");
        }
        #[cfg(feature = "aws-sdk")]
        let b = crate::store::aws::complete(b, bucket, profile.as_deref())
            .await
            .map_err(bad)?;
        #[cfg(not(feature = "aws-sdk"))]
        if profile.is_some() {
            return Err(bad(format!(
                "{AWS_PROFILE} needs moraine's aws-sdk feature"
            )));
        }
        let store = b.build().map_err(|e| bad(e.to_string()))?;
        let url = Url::parse(u).map_err(|e| bad(e.to_string()))?;
        return Ok((Arc::new(store), Path::from(prefix), url));
    }
    if u.contains("://") && !u.starts_with("file://") {
        return Err(bad("unsupported scheme".into()));
    }
    let dir = u.strip_prefix("file://").unwrap_or(u);
    let dir = std::path::absolute(dir).map_err(|e| bad(e.to_string()))?;
    std::fs::create_dir_all(&dir).map_err(|e| bad(e.to_string()))?;
    let fs = object_store::local::LocalFileSystem::new();
    let url = Url::from_directory_path(&dir).map_err(|_| bad("not a directory path".into()))?;
    let prefix = Path::from_filesystem_path(&dir).map_err(|e| bad(e.to_string()))?;
    Ok((Arc::new(LocalCasStore::new(Arc::new(fs))), prefix, url))
}
