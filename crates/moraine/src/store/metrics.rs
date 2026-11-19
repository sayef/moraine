//! Request and byte counting per operation (D18: requests per commit are measured).

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyMode, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result,
};

/// The request kinds counted separately.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Op {
    /// A whole-object GET.
    Get,
    /// A ranged GET.
    GetRange,
    /// A HEAD.
    Head,
    /// An unconditional PUT.
    PutOverwrite,
    /// A put-if-absent.
    PutCreate,
    /// A compare-and-swap PUT.
    PutUpdate,
    /// Starting a multipart upload.
    Multipart,
    /// One part of a multipart upload.
    Part,
    /// Completing a multipart upload.
    Complete,
    /// A recursive listing (one request per page).
    List,
    /// A listing of one level, with common prefixes.
    ListDelimiter,
    /// A DELETE.
    Delete,
    /// A copy.
    Copy,
    /// A copy-if-absent.
    CopyIfNotExists,
    /// A rename.
    Rename,
}

impl Op {
    const ALL: [Op; 15] = [
        Op::Get,
        Op::GetRange,
        Op::Head,
        Op::PutOverwrite,
        Op::PutCreate,
        Op::PutUpdate,
        Op::Multipart,
        Op::Part,
        Op::Complete,
        Op::List,
        Op::ListDelimiter,
        Op::Delete,
        Op::Copy,
        Op::CopyIfNotExists,
        Op::Rename,
    ];

    /// Every kind, in a fixed order.
    pub fn all() -> &'static [Op] {
        &Op::ALL
    }

    /// The kind's name in reports, such as `put_create`.
    pub fn name(self) -> &'static str {
        match self {
            Op::Get => "get",
            Op::GetRange => "get_range",
            Op::Head => "head",
            Op::PutOverwrite => "put",
            Op::PutCreate => "put_create",
            Op::PutUpdate => "put_update",
            Op::Multipart => "multipart",
            Op::Part => "part",
            Op::Complete => "complete",
            Op::List => "list",
            Op::ListDelimiter => "list_delimiter",
            Op::Delete => "delete",
            Op::Copy => "copy",
            Op::CopyIfNotExists => "copy_if_not_exists",
            Op::Rename => "rename",
        }
    }

    /// The kind of a `get_opts` call.
    pub fn of_get(opts: &GetOptions) -> Op {
        if opts.head {
            Op::Head
        } else if opts.range.is_some() {
            Op::GetRange
        } else {
            Op::Get
        }
    }

    /// The kind of a `put_opts` call.
    pub fn of_put(mode: &PutMode) -> Op {
        match mode {
            PutMode::Overwrite => Op::PutOverwrite,
            PutMode::Create => Op::PutCreate,
            PutMode::Update(_) => Op::PutUpdate,
        }
    }

    fn index(self) -> usize {
        Op::all().iter().position(|o| *o == self).unwrap_or(0)
    }
}

/// Counters shared by every clone of a [`MetricsStore`].
#[derive(Default)]
pub struct StoreMetrics {
    requests: [AtomicU64; 15],
    errors: [AtomicU64; 15],
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
}

impl fmt::Debug for StoreMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.snapshot().fmt(f)
    }
}

impl StoreMetrics {
    /// Counts a request of kind `op` (for store wrappers that count what they send).
    pub fn record(&self, op: Op) {
        self.requests[op.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Counts a failed request of kind `op`.
    pub fn record_error(&self, op: Op) {
        self.errors[op.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Counts `n` bytes received.
    pub fn add_read(&self, n: u64) {
        self.bytes_read.fetch_add(n, Ordering::Relaxed);
    }

    /// Counts `n` bytes sent.
    pub fn add_written(&self, n: u64) {
        self.bytes_written.fetch_add(n, Ordering::Relaxed);
    }

    /// The counters now. Subtract two snapshots to count one operation.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut by_op = BTreeMap::new();
        for &op in Op::all() {
            let requests = self.requests[op.index()].load(Ordering::Relaxed);
            let errors = self.errors[op.index()].load(Ordering::Relaxed);
            if requests > 0 || errors > 0 {
                by_op.insert(op, OpCount { requests, errors });
            }
        }
        MetricsSnapshot {
            by_op,
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
        }
    }
}

/// Requests and failed requests of one kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct OpCount {
    /// Requests sent, retries included.
    pub requests: u64,
    /// Requests that failed.
    pub errors: u64,
}

/// A point-in-time copy of [`StoreMetrics`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct MetricsSnapshot {
    /// Counts by request kind; kinds never sent are absent.
    pub by_op: BTreeMap<Op, OpCount>,
    /// Bytes received in response bodies.
    pub bytes_read: u64,
    /// Bytes sent in request bodies.
    pub bytes_written: u64,
}

impl MetricsSnapshot {
    /// All requests sent.
    pub fn requests(&self) -> u64 {
        self.by_op.values().map(|c| c.requests).sum()
    }

    /// Requests of one kind.
    pub fn count(&self, op: Op) -> u64 {
        self.by_op.get(&op).map_or(0, |c| c.requests)
    }

    /// Listings of either kind.
    pub fn listings(&self) -> u64 {
        self.count(Op::List) + self.count(Op::ListDelimiter)
    }

    /// What happened between `earlier` and `self`.
    pub fn since(&self, earlier: &MetricsSnapshot) -> MetricsSnapshot {
        let mut by_op = BTreeMap::new();
        for (op, c) in &self.by_op {
            let e = earlier.by_op.get(op).copied().unwrap_or_default();
            let d = OpCount {
                requests: c.requests - e.requests,
                errors: c.errors - e.errors,
            };
            if d != OpCount::default() {
                by_op.insert(*op, d);
            }
        }
        MetricsSnapshot {
            by_op,
            bytes_read: self.bytes_read - earlier.bytes_read,
            bytes_written: self.bytes_written - earlier.bytes_written,
        }
    }
}

impl fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} requests (", self.requests())?;
        for (i, (op, c)) in self.by_op.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{} {}", op.name(), c.requests)?;
        }
        write!(
            f,
            "), {} B read, {} B written",
            self.bytes_read, self.bytes_written
        )
    }
}

/// Counts every request that reaches the wrapped store.
#[derive(Debug)]
pub struct MetricsStore {
    inner: Arc<dyn ObjectStore>,
    metrics: Arc<StoreMetrics>,
}

impl MetricsStore {
    /// Counts what goes through `inner` in `metrics`.
    pub fn new(inner: Arc<dyn ObjectStore>, metrics: Arc<StoreMetrics>) -> Self {
        MetricsStore { inner, metrics }
    }

    fn done<T>(&self, op: Op, r: Result<T>) -> Result<T> {
        if r.is_err() {
            self.metrics.record_error(op);
        }
        r
    }
}

/// A multipart upload whose parts and completion are counted.
#[derive(Debug)]
struct CountedUpload {
    inner: Box<dyn MultipartUpload>,
    metrics: Arc<StoreMetrics>,
}

#[async_trait]
impl MultipartUpload for CountedUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        self.metrics.record(Op::Part);
        self.metrics.add_written(data.content_length() as u64);
        let metrics = self.metrics.clone();
        let fut = self.inner.put_part(data);
        Box::pin(async move {
            let r = fut.await;
            if r.is_err() {
                metrics.record_error(Op::Part);
            }
            r
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        self.metrics.record(Op::Complete);
        let r = self.inner.complete().await;
        if r.is_err() {
            self.metrics.record_error(Op::Complete);
        }
        r
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

impl fmt::Display for MetricsStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Metrics({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for MetricsStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let op = Op::of_put(&opts.mode);
        self.metrics.record(op);
        self.metrics.add_written(payload.content_length() as u64);
        let r = self.inner.put_opts(location, payload, opts).await;
        self.done(op, r)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        // The initiation here; each part and the completion as they happen.
        self.metrics.record(Op::Multipart);
        let r = self.inner.put_multipart_opts(location, opts).await;
        let up = self.done(Op::Multipart, r)?;
        Ok(Box::new(CountedUpload {
            inner: up,
            metrics: self.metrics.clone(),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let op = Op::of_get(&options);
        self.metrics.record(op);
        let r = self.inner.get_opts(location, options).await;
        let mut r = self.done(op, r)?;
        // Count body bytes as they are consumed. Local files are counted by their range.
        match r.payload {
            GetResultPayload::Stream(s) => {
                let m = self.metrics.clone();
                r.payload = GetResultPayload::Stream(
                    s.inspect(move |b| {
                        if let Ok(b) = b {
                            m.add_read(b.len() as u64);
                        }
                    })
                    .boxed(),
                );
            }
            #[allow(unreachable_patterns)]
            _ => self.metrics.add_read(r.range.end - r.range.start),
        }
        Ok(r)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        // Counted as the stores send it: one request per range (object_store coalesces
        // neighbouring ranges first in its own implementations, which this does not see).
        for _ in ranges {
            self.metrics.record(Op::GetRange);
        }
        let r = self.inner.get_ranges(location, ranges).await;
        let r = self.done(Op::GetRange, r)?;
        self.metrics
            .add_read(r.iter().map(|b| b.len() as u64).sum::<u64>());
        Ok(r)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let m = self.metrics.clone();
        let counted = locations
            .inspect(move |p| {
                if p.is_ok() {
                    m.record(Op::Delete);
                }
            })
            .boxed();
        let m = self.metrics.clone();
        self.inner
            .delete_stream(counted)
            .inspect(move |r| {
                if r.is_err() {
                    m.record_error(Op::Delete);
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        // One request per listing call; pagination is not visible at this level.
        self.metrics.record(Op::List);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.metrics.record(Op::List);
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.metrics.record(Op::ListDelimiter);
        let r = self.inner.list_with_delimiter(prefix).await;
        self.done(Op::ListDelimiter, r)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let op = match options.mode {
            CopyMode::Overwrite => Op::Copy,
            CopyMode::Create => Op::CopyIfNotExists,
        };
        self.metrics.record(op);
        let r = self.inner.copy_opts(from, to, options).await;
        self.done(op, r)
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.metrics.record(Op::Rename);
        let r = self.inner.rename_opts(from, to, options).await;
        self.done(Op::Rename, r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn counts_requests_and_bytes() {
        let m = Arc::new(StoreMetrics::default());
        let s = MetricsStore::new(Arc::new(InMemory::new()), m.clone());
        let p = Path::from("a/b");
        let before = m.snapshot();
        s.put(&p, PutPayload::from_static(b"hello")).await.unwrap();
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        assert!(s.put_opts(&p, "x".into(), opts).await.is_err());
        let body = s.get(&p).await.unwrap().bytes().await.unwrap();
        assert_eq!(&body[..], b"hello");
        s.head(&p).await.unwrap();
        let d = m.snapshot().since(&before);
        assert_eq!(d.count(Op::PutOverwrite), 1);
        assert_eq!(d.count(Op::PutCreate), 1);
        assert_eq!(d.by_op[&Op::PutCreate].errors, 1);
        assert_eq!(d.count(Op::Get), 1);
        assert_eq!(d.count(Op::Head), 1);
        assert_eq!(d.requests(), 4);
        assert_eq!(d.bytes_read, 5);
        assert_eq!(d.bytes_written, 6);
    }
}
