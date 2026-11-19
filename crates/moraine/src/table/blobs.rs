//! Blob reads and staged uploads of a table handle ([`crate::blob`]).

use arrow::array::RecordBatch;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};

use super::{Table, TableInner};
use crate::blob::{self, Blob, BlobRef, PART_BYTES, SINGLE_PUT_BYTES};
use crate::error::{Error, Result};

impl TableInner {
    /// `<db root>/<table>/blobs`, a sibling of `data/` that delta-rs never sees.
    pub(crate) fn blob_prefix(&self) -> Path {
        self.db.root.clone().join(self.name.as_str()).join("blobs")
    }
}

impl Table {
    /// A handle on the blob a row holds, for reading.
    ///
    /// ```no_run
    /// # async fn f(t: moraine::Table) -> moraine::Result<()> {
    /// use moraine::expr::{col, lit};
    /// let rows = t.scan().filter(col("id").eq(lit(7))).collect().await?;
    /// for r in moraine::blob::refs(&rows[0], "body")?.into_iter().flatten() {
    ///     let blob = t.blob(&r)?;
    ///     let head = blob.range(0..16.min(blob.size())).await?; // one ranged GET
    ///     let all = blob.bytes().await?; // checksum verified
    ///     assert_eq!(&all[..head.len()], &head[..]);
    /// }
    /// # Ok(()) }
    /// ```
    pub fn blob(&self, r: &BlobRef) -> Result<Blob> {
        Blob::new(
            self.inner.db.store.object_store().clone(),
            &self.inner.blob_prefix(),
            r.clone(),
        )
    }

    /// The blobs of column `column` of `batch` (rows read from this table), `None` for nulls.
    pub fn blobs(&self, batch: &RecordBatch, column: &str) -> Result<Vec<Option<Blob>>> {
        blob::refs(batch, column)?
            .into_iter()
            .map(|r| r.map(|r| self.blob(&r)).transpose())
            .collect()
    }

    /// Uploads a value from a stream ahead of a write, in parts of 16 MiB, so memory stays at
    /// two parts whatever the value's size (case B6). Returns the reference to put in a row
    /// with [`blob::BlobColumn::push_ref`].
    ///
    /// The object is not referenced until a write that holds the reference commits. A write
    /// through this handle commits the reference as it is while it is within the commit
    /// deadline of the upload's start (a quarter of the grace period) and copies it to a fresh
    /// key after that; any other handle always copies it. An upload that is
    /// never committed is an orphan, collected after the grace period. A failed upload is
    /// aborted and leaves nothing visible (case B7).
    ///
    /// ```no_run
    /// # async fn f(t: moraine::Table, schema: arrow::datatypes::SchemaRef) -> moraine::Result<()> {
    /// use moraine::blob::BlobColumn;
    /// let chunks = (0..64).map(|_| Ok::<_, std::io::Error>(bytes::Bytes::from(vec![7u8; 1 << 20])));
    /// let chunks = futures::stream::iter(chunks);
    /// let r = t.upload_blob(chunks, Some("application/octet-stream")).await?;
    /// let mut body = BlobColumn::new();
    /// body.push_ref(&r);
    /// let ids: arrow::array::ArrayRef = std::sync::Arc::new(arrow::array::Int64Array::from(vec![1]));
    /// let batch = arrow::array::RecordBatch::try_new(schema, vec![ids, body.finish()])?;
    /// t.append(vec![batch]).await?;
    /// # Ok(()) }
    /// ```
    pub async fn upload_blob<S, E>(&self, body: S, content_type: Option<&str>) -> Result<BlobRef>
    where
        S: Stream<Item = std::result::Result<Bytes, E>> + Send,
        E: std::fmt::Display,
    {
        let inner = &self.inner;
        inner.db.require_writable().await?;
        let clock = inner.db.clock().clone();
        let store = inner.db.store.object_store().clone();
        let key = blob::new_key(clock.as_ref());
        let location = blob::join_key(&inner.blob_prefix(), &key);
        let started = clock.now();
        let mut body = std::pin::pin!(body);
        let mut buf = BytesMut::new();
        let mut crc = 0u32;
        let mut size = 0u64;
        // Parts go up concurrently, up to [`blob::PARTS_IN_FLIGHT`] at once (D82); the memory
        // held is the parts in flight and the part being filled.
        let mut upload: Option<blob::PartUpload> = None;
        let fail = |e: E| Error::InvalidBatch(format!("blob stream: {e}"));
        loop {
            let next = body.next().await;
            let chunk = match next {
                Some(Ok(c)) => Some(c),
                Some(Err(e)) => {
                    if let Some(up) = upload.take() {
                        up.abort().await;
                    }
                    return Err(fail(e));
                }
                None => None,
            };
            let end = chunk.is_none();
            if let Some(c) = chunk {
                crc = crc32c::crc32c_append(crc, &c);
                size += c.len() as u64;
                buf.extend_from_slice(&c);
            }
            // A value that fits one PUT goes up in one.
            if end && upload.is_none() && buf.len() <= SINGLE_PUT_BYTES {
                store.put(&location, PutPayload::from(buf.freeze())).await?;
                break;
            }
            while buf.len() >= PART_BYTES || (end && !buf.is_empty()) {
                let n = buf.len().min(PART_BYTES);
                let part = buf.split_to(n).freeze();
                if upload.is_none() {
                    upload = Some(blob::PartUpload::new(
                        store.put_multipart(&location).await?,
                        blob::PARTS_IN_FLIGHT,
                    ));
                }
                let Some(up) = upload.as_mut() else { break };
                if let Err(e) = up.put(part).await {
                    if let Some(up) = upload.take() {
                        up.abort().await;
                    }
                    return Err(e);
                }
            }
            if end {
                if let Some(mut up) = upload.take()
                    && let Err(e) = up.complete().await
                {
                    up.abort().await;
                    return Err(e);
                }
                break;
            }
        }
        fail::fail_point!("moraine::blob::after_stage");
        inner.staged.insert(key.clone(), started);
        Ok(BlobRef::external(
            key,
            size,
            crc,
            content_type.map(str::to_string),
        ))
    }
}
