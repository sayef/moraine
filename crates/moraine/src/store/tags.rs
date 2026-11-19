//! Object tags on every write (D29): a wrapper that adds a database's configured tags to every
//! put and multipart upload it forwards.
//!
//! Tags travel with the write request itself (S3's `x-amz-tagging` header, Azure's
//! `x-ms-tags`), so they cost no request of their own. Stores without tags (the local
//! filesystem, the in-memory store) ignore them. Copies keep the source object's tags on S3.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result, TagSet,
};

/// Adds a fixed set of tags to every put and multipart upload. Tags a caller already set on a
/// request are kept, and the configured ones are appended after them.
#[derive(Debug)]
pub struct TagStore {
    inner: Arc<dyn ObjectStore>,
    tags: Vec<(String, String)>,
}

impl TagStore {
    /// Wraps `inner`, adding `tags` to every object it puts.
    pub fn new(inner: Arc<dyn ObjectStore>, tags: Vec<(String, String)>) -> Self {
        TagStore { inner, tags }
    }

    fn add(&self, set: &mut TagSet) {
        for (k, v) in &self.tags {
            set.push(k, v);
        }
    }
}

impl fmt::Display for TagStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TagStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for TagStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> Result<PutResult> {
        self.add(&mut opts.tags);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        mut opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.add(&mut opts.tags);
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
