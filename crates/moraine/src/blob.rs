//! Blobs: binary values stored inside the row when small and as objects of
//! their own beside the table when large, with the row as the only record of which are alive.
//!
//! A blob column is an Arrow struct ([`data_type`]) of five fields: `inline` (the bytes, when
//! at or below the table's inline threshold), `key` (the object under the table's `blobs/`
//! prefix otherwise), `size`, `crc32c` and `content_type`. Exactly one of `inline` and `key` is
//! set in a non-null value.
//!
//! Writing: build the column with [`BlobColumn`], from bytes ([`BlobColumn::push_bytes`]) or
//! from references read from rows ([`BlobColumn::push_ref`]). Every write moves values above the
//! threshold out to objects of their own (one PUT each, a multipart upload above 16 MiB) before
//! it uploads its data files. A reference the write did not upload itself is *pass-through*: it
//! is copied on the server to a fresh key, so a commit only ever references objects younger than
//! its commit deadline or referenced all along, which is what makes collection safe (case B3).
//! [`crate::Table::upload_blob`] streams a large value up ahead of the write, with bounded
//! memory; the handle that uploaded it may commit the reference without a copy within the
//! commit deadline of the upload, and copies it after that.
//!
//! Reading: [`crate::Table::blob`] returns a [`Blob`] handle with [`Blob::bytes`] (the checksum
//! is verified), [`Blob::range`] and [`Blob::stream`]. A missing object is
//! [`Error::BlobNotFound`] for that value only (case B4); a checksum mismatch is
//! [`Error::BlobCorrupt`] and never returns wrong bytes (case B10).
//!
//! Collection runs with vacuum: an object under `blobs/` that no row of a kept data file
//! references (vacuum's keep set: the current files, files a pin names and files removed less
//! than the grace period ago) and that is older than the grace period by server time is deleted.

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BinaryBuilder, Int64Array, Int64Builder, RecordBatch,
    StringArray, StringBuilder, StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Fields, Int64Type};
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::clock::Clock;
use crate::error::{Error, Result};
use crate::names::NotFound;

/// The struct field of a blob column holding the bytes of a value stored in the row.
pub const INLINE: &str = "inline";
/// The struct field holding the object key of a value stored outside the row.
pub const KEY: &str = "key";
/// The struct field holding the value's size in bytes.
pub const SIZE: &str = "size";
/// The struct field holding the CRC-32C of the value's bytes.
pub const CRC32C: &str = "crc32c";
/// The struct field holding the value's content type, if one was given.
pub const CONTENT_TYPE: &str = "content_type";

/// Values above this go up as a multipart upload.
pub(crate) const SINGLE_PUT_BYTES: usize = 16 << 20;
/// Part size of a multipart blob upload.
pub(crate) const PART_BYTES: usize = 16 << 20;
/// Parts of one multipart blob upload in flight at once (D82): memory in flight is at most
/// this many parts.
pub(crate) const PARTS_IN_FLIGHT: usize = 4;
/// Bytes per ranged GET of a streamed read (D82).
pub(crate) const STREAM_CHUNK_BYTES: u64 = 4 << 20;
/// Ranged GETs a streamed read keeps in flight ahead of its consumer (D82); a stream holds at
/// most this many chunks.
pub(crate) const STREAM_READ_AHEAD: usize = 4;
/// The default inline threshold of a table.
pub const DEFAULT_INLINE_BYTES: u64 = 256 << 10;

/// What a write does with a blob reference it did not upload itself.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum BlobReuse {
    /// Copy the object to a fresh key on the server (no bytes through the client).
    #[default]
    Copy,
    /// Commit the reference as it is. Only safe when the caller knows the object has been
    /// referenced by a row without a gap since it was uploaded; otherwise collection may delete
    /// it under the new row.
    TrustCaller,
}

fn children() -> Fields {
    Fields::from(vec![
        Field::new(INLINE, DataType::Binary, true),
        Field::new(KEY, DataType::Utf8, true),
        Field::new(SIZE, DataType::Int64, true),
        Field::new(CRC32C, DataType::Int64, true),
        Field::new(CONTENT_TYPE, DataType::Utf8, true),
    ])
}

/// The Arrow type of a blob column.
pub fn data_type() -> DataType {
    DataType::Struct(children())
}

/// A blob column named `name`.
pub fn field(name: &str, nullable: bool) -> Field {
    Field::new(name, data_type(), nullable)
}

/// Whether `t` is the blob type: a struct with exactly the blob fields, in order, of the blob
/// types (nullability aside).
pub fn is_blob(t: &DataType) -> bool {
    let DataType::Struct(fs) = t else {
        return false;
    };
    let want = children();
    fs.len() == want.len()
        && fs
            .iter()
            .zip(want.iter())
            .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type())
}

/// The CRC-32C of `bytes`.
pub fn crc32c(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

/// One blob value as a row holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobRef {
    inline: Option<Bytes>,
    key: Option<String>,
    size: u64,
    crc32c: u32,
    content_type: Option<String>,
}

impl BlobRef {
    /// Length in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// CRC-32C of the bytes.
    pub fn crc32c(&self) -> u32 {
        self.crc32c
    }

    /// The content type given when the value was written.
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    /// The object key under the table's `blobs/` prefix, when the value is stored outside the
    /// row.
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    /// Whether the bytes are inside the row.
    pub fn is_inline(&self) -> bool {
        self.inline.is_some()
    }

    /// The bytes, when inline.
    pub fn inline_bytes(&self) -> Option<&Bytes> {
        self.inline.as_ref()
    }

    /// A reference to an external object, for [`BlobColumn::push_ref`].
    pub fn external(
        key: impl Into<String>,
        size: u64,
        crc32c: u32,
        content_type: Option<String>,
    ) -> BlobRef {
        BlobRef {
            inline: None,
            key: Some(key.into()),
            size,
            crc32c,
            content_type,
        }
    }
}

/// The values of blob column `column` of `batch`, `None` for nulls.
pub fn refs(batch: &RecordBatch, column: &str) -> Result<Vec<Option<BlobRef>>> {
    let a = batch
        .column_by_name(column)
        .ok_or_else(|| Error::InvalidQuery(format!("no column {column:?} in the batch")))?;
    refs_of(a.as_ref())
}

fn refs_of(a: &dyn Array) -> Result<Vec<Option<BlobRef>>> {
    let s = Parts::of(a)?;
    Ok((0..a.len()).map(|i| s.get(i)).collect())
}

/// Typed views of a blob struct array's fields.
struct Parts<'a> {
    nulls: Option<&'a NullBuffer>,
    inline: &'a BinaryArray,
    key: &'a StringArray,
    size: &'a Int64Array,
    crc: &'a Int64Array,
    ct: &'a StringArray,
}

impl<'a> Parts<'a> {
    fn of(a: &'a dyn Array) -> Result<Parts<'a>> {
        if !is_blob(a.data_type()) {
            return Err(Error::InvalidBatch(format!(
                "not a blob column: type {}",
                a.data_type()
            )));
        }
        let s = a.as_struct();
        Ok(Parts {
            nulls: s.nulls(),
            inline: s.column(0).as_binary::<i32>(),
            key: s.column(1).as_string::<i32>(),
            size: s.column(2).as_primitive::<Int64Type>(),
            crc: s.column(3).as_primitive::<Int64Type>(),
            ct: s.column(4).as_string::<i32>(),
        })
    }

    fn get(&self, i: usize) -> Option<BlobRef> {
        if self.nulls.is_some_and(|n| n.is_null(i)) {
            return None;
        }
        let opt_str = |a: &StringArray| (!a.is_null(i)).then(|| a.value(i).to_string());
        Some(BlobRef {
            inline: (!self.inline.is_null(i)).then(|| Bytes::copy_from_slice(self.inline.value(i))),
            key: opt_str(self.key),
            size: if self.size.is_null(i) {
                0
            } else {
                self.size.value(i).max(0) as u64
            },
            crc32c: if self.crc.is_null(i) {
                0
            } else {
                self.crc.value(i) as u32
            },
            content_type: opt_str(self.ct),
        })
    }

    /// The external key of row `i`, if any.
    fn key(&self, i: usize) -> Option<&'a str> {
        if self.nulls.is_some_and(|n| n.is_null(i)) || self.key.is_null(i) {
            None
        } else {
            Some(self.key.value(i))
        }
    }
}

/// Builds a blob column.
///
/// ```
/// use moraine::blob::{BlobColumn, field};
/// use std::sync::Arc;
/// let mut c = BlobColumn::new();
/// c.push_bytes(&b"hello"[..], Some("text/plain"));
/// c.push_null();
/// let schema = Arc::new(arrow::datatypes::Schema::new(vec![field("body", true)]));
/// let batch = arrow::array::RecordBatch::try_new(schema, vec![c.finish()]).unwrap();
/// assert_eq!(batch.num_rows(), 2);
/// ```
#[derive(Default)]
pub struct BlobColumn {
    inline: BinaryBuilder,
    key: StringBuilder,
    size: Int64Builder,
    crc: Int64Builder,
    ct: StringBuilder,
    valid: Vec<bool>,
}

impl BlobColumn {
    /// An empty column.
    pub fn new() -> BlobColumn {
        BlobColumn::default()
    }

    /// A value given as bytes, of any size: the write keeps it inline or uploads it.
    pub fn push_bytes(&mut self, bytes: impl AsRef<[u8]>, content_type: Option<&str>) {
        let b = bytes.as_ref();
        self.inline.append_value(b);
        self.key.append_null();
        self.size.append_value(b.len() as i64);
        self.crc.append_value(i64::from(crc32c(b)));
        self.ct.append_option(content_type);
        self.valid.push(true);
    }

    /// A value as another row holds it, inline or external. An external reference is copied
    /// to a fresh key when written (unless [`BlobReuse::TrustCaller`]).
    pub fn push_ref(&mut self, r: &BlobRef) {
        self.inline.append_option(r.inline.as_deref());
        self.key.append_option(r.key.as_deref());
        self.size.append_value(r.size as i64);
        self.crc.append_value(i64::from(r.crc32c));
        self.ct.append_option(r.content_type.as_deref());
        self.valid.push(true);
    }

    /// A null value.
    pub fn push_null(&mut self) {
        self.inline.append_null();
        self.key.append_null();
        self.size.append_null();
        self.crc.append_null();
        self.ct.append_null();
        self.valid.push(false);
    }

    /// Values pushed so far.
    pub fn len(&self) -> usize {
        self.valid.len()
    }

    /// Whether no value has been pushed.
    pub fn is_empty(&self) -> bool {
        self.valid.is_empty()
    }

    /// The column, typed as [`data_type`], to put in a batch.
    pub fn finish(mut self) -> ArrayRef {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.inline.finish()),
            Arc::new(self.key.finish()),
            Arc::new(self.size.finish()),
            Arc::new(self.crc.finish()),
            Arc::new(self.ct.finish()),
        ];
        let nulls = self
            .valid
            .iter()
            .any(|v| !v)
            .then(|| NullBuffer::from(self.valid.clone()));
        Arc::new(StructArray::new(children(), arrays, nulls))
    }
}

/// A key is two hex characters, a slash and a UUID: nothing else may name an object.
pub(crate) fn valid_key(k: &str) -> bool {
    let b = k.as_bytes();
    b.len() == 39
        && b[2] == b'/'
        && b[..2]
            .iter()
            .all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'))
        && b[3..].iter().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => *c == b'-',
            _ => matches!(c, b'0'..=b'9' | b'a'..=b'f'),
        })
}

/// A fresh key: a UUID v7 from `clock` (time-ordered, unique without coordination), under a
/// directory named by two hex characters of its random part, so keys spread over prefixes.
pub(crate) fn new_key(clock: &dyn Clock) -> String {
    let ms = clock.wall_now().timestamp_millis().max(0) as u64;
    let (a, b) = (clock.random_u64(), clock.random_u64());
    let hi = (ms << 16) | 0x7000 | (a & 0x0fff);
    let lo = (b & 0x3fff_ffff_ffff_ffff) | 0x8000_0000_0000_0000;
    let u = format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        hi >> 32,
        (hi >> 16) & 0xffff,
        hi & 0xffff,
        lo >> 48,
        lo & 0xffff_ffff_ffff
    );
    format!("{}/{u}", &u[34..36])
}

/// A blob of a row, ready to read. From [`crate::Table::blob`].
#[derive(Clone)]
pub struct Blob {
    store: Arc<dyn ObjectStore>,
    location: Option<Path>,
    r: BlobRef,
}

impl std::fmt::Debug for Blob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Blob")
            .field("key", &self.r.key)
            .field("size", &self.r.size)
            .finish()
    }
}

impl Blob {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, prefix: &Path, r: BlobRef) -> Result<Blob> {
        let location = match &r.key {
            Some(k) if valid_key(k) => Some(join_key(prefix, k)),
            Some(k) => {
                return Err(Error::BlobNotFound(NotFound::bare(format!(
                    "invalid blob key {k:?}"
                ))));
            }
            None => None,
        };
        Ok(Blob { store, location, r })
    }

    /// The value's size in bytes.
    pub fn size(&self) -> u64 {
        self.r.size
    }

    /// The content type given when the value was written.
    pub fn content_type(&self) -> Option<&str> {
        self.r.content_type()
    }

    /// The reference the row holds.
    pub fn reference(&self) -> &BlobRef {
        &self.r
    }

    fn not_found(&self) -> Error {
        Error::BlobNotFound(NotFound::bare(self.r.key.clone().unwrap_or_default()))
    }

    fn map_err(&self, e: object_store::Error) -> Error {
        match e {
            object_store::Error::NotFound { .. } => self.not_found(),
            e => Error::from(e),
        }
    }

    fn check(&self, b: &[u8]) -> Result<()> {
        if b.len() as u64 != self.r.size || crc32c(b) != self.r.crc32c {
            return Err(Error::BlobCorrupt(format!(
                "blob {} has {} bytes with CRC-32C {:08x}; the row says {} bytes, {:08x}",
                self.r.key.as_deref().unwrap_or("(inline)"),
                b.len(),
                crc32c(b),
                self.r.size,
                self.r.crc32c
            )));
        }
        Ok(())
    }

    /// The whole value, its checksum verified.
    pub async fn bytes(&self) -> Result<Bytes> {
        let b = match (&self.r.inline, &self.location) {
            (Some(b), _) => b.clone(),
            (None, Some(loc)) => self
                .store
                .get(loc)
                .await
                .map_err(|e| self.map_err(e))?
                .bytes()
                .await
                .map_err(|e| self.map_err(e))?,
            (None, None) => return Err(self.not_found()),
        };
        self.check(&b)?;
        Ok(b)
    }

    /// Bytes `range` of the value, read with one ranged GET. A part of a value cannot be
    /// checked against the whole value's checksum; its length is.
    pub async fn range(&self, range: Range<u64>) -> Result<Bytes> {
        if range.start > range.end || range.end > self.r.size {
            return Err(Error::InvalidQuery(format!(
                "range {range:?} is outside a blob of {} bytes",
                self.r.size
            )));
        }
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        let b = match (&self.r.inline, &self.location) {
            (Some(b), _) => {
                if b.len() as u64 != self.r.size {
                    return self.check(b).map(|_| Bytes::new());
                }
                b.slice(range.start as usize..range.end as usize)
            }
            (None, Some(loc)) => self
                .store
                .get_range(loc, range.clone())
                .await
                .map_err(|e| self.map_err(e))?,
            (None, None) => return Err(self.not_found()),
        };
        if b.len() as u64 != range.end - range.start {
            return Err(Error::BlobCorrupt(format!(
                "range {range:?} of blob {} returned {} bytes",
                self.r.key.as_deref().unwrap_or("(inline)"),
                b.len()
            )));
        }
        Ok(b)
    }

    /// The value as a stream of chunks of 4 MiB, read with ranged GETs of which up to four
    /// run ahead of the consumer (D82), so memory stays at a
    /// few chunks. The first GET also checks the object's size. The checksum is verified at
    /// the end: a mismatch is the stream's last item, an error, so a consumer that reaches the
    /// end without one has the right bytes.
    pub async fn stream(&self) -> Result<BoxStream<'static, Result<Bytes>>> {
        match (&self.r.inline, &self.location) {
            (Some(b), _) => {
                self.check(b)?;
                Ok(futures::stream::iter([Ok(b.clone())]).boxed())
            }
            (None, Some(loc)) => {
                let size = self.r.size;
                let first = size.min(STREAM_CHUNK_BYTES);
                let head = self
                    .store
                    .get_opts(
                        loc,
                        object_store::GetOptions {
                            range: (first > 0).then_some((0..first).into()),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|e| self.map_err(e))?;
                if head.meta.size != size {
                    return Err(Error::BlobCorrupt(format!(
                        "blob {} has {} bytes; the row says {size}",
                        self.r.key.as_deref().unwrap_or(""),
                        head.meta.size
                    )));
                }
                let head = head.bytes().await.map_err(|e| self.map_err(e))?;
                let me = self.clone();
                let state = Arc::new(Mutex::new((crc32c::crc32c(&[]), 0u64)));
                let s2 = state.clone();
                let (store, loc) = (self.store.clone(), loc.clone());
                let rest = (first..size)
                    .step_by(STREAM_CHUNK_BYTES as usize)
                    .map(move |at| at..(at + STREAM_CHUNK_BYTES).min(size));
                let fetch = me.clone();
                let rest = futures::stream::iter(rest)
                    .map(move |r| {
                        let (store, loc, me) = (store.clone(), loc.clone(), fetch.clone());
                        async move { store.get_range(&loc, r).await.map_err(|e| me.map_err(e)) }
                    })
                    .buffered(STREAM_READ_AHEAD);
                let body = futures::stream::once(futures::future::ready(Ok(head)))
                    .chain(rest)
                    .try_filter(|b| futures::future::ready(!b.is_empty()))
                    .map(move |c: Result<Bytes>| {
                        let c = c?;
                        if let Ok(mut g) = s2.lock() {
                            g.0 = crc32c::crc32c_append(g.0, &c);
                            g.1 += c.len() as u64;
                        }
                        Ok(c)
                    });
                let tail = futures::stream::once(async move {
                    let (crc, len) = state.lock().map(|g| *g).unwrap_or((0, 0));
                    if len != me.r.size || crc != me.r.crc32c {
                        Err(Error::BlobCorrupt(format!(
                            "blob {} streamed {len} bytes with CRC-32C {crc:08x}; the row says \
                             {} bytes, {:08x}",
                            me.r.key.as_deref().unwrap_or(""),
                            me.r.size,
                            me.r.crc32c
                        )))
                    } else {
                        Ok(Bytes::new())
                    }
                })
                .try_filter(|b| futures::future::ready(!b.is_empty()));
                Ok(body.chain(tail).boxed())
            }
            (None, None) => Err(self.not_found()),
        }
    }
}

pub(crate) fn join_key(prefix: &Path, key: &str) -> Path {
    let (dir, name) = key.split_at(2);
    prefix.clone().join(dir).join(&name[1..])
}

/// Uploads `body` to `location`: one PUT up to [`SINGLE_PUT_BYTES`], a multipart upload of
/// [`PART_BYTES`] parts above, up to [`PARTS_IN_FLIGHT`] at once (the parts slice `body`,
/// nothing is copied).
pub(crate) async fn put_bytes(
    store: &Arc<dyn ObjectStore>,
    location: &Path,
    body: Bytes,
) -> Result<()> {
    if body.len() <= SINGLE_PUT_BYTES {
        store.put(location, PutPayload::from(body)).await?;
        return Ok(());
    }
    let mut up = PartUpload::new(store.put_multipart(location).await?, PARTS_IN_FLIGHT);
    let sent = async {
        let mut at = 0;
        while at < body.len() {
            let end = (at + PART_BYTES).min(body.len());
            up.put(body.slice(at..end)).await?;
            at = end;
        }
        up.complete().await
    }
    .await;
    if sent.is_err() {
        up.abort().await;
    }
    sent
}

/// A multipart upload whose parts go up concurrently, at most `max` at once (D82). Part
/// numbers follow the order of [`PartUpload::put`] calls whatever order the parts finish in.
pub(crate) struct PartUpload {
    up: Box<dyn object_store::MultipartUpload>,
    running: futures::stream::FuturesUnordered<object_store::UploadPart>,
    max: usize,
}

impl PartUpload {
    pub fn new(up: Box<dyn object_store::MultipartUpload>, max: usize) -> PartUpload {
        PartUpload {
            up,
            running: futures::stream::FuturesUnordered::new(),
            max: max.max(1),
        }
    }

    /// Starts the next part, first waiting for room.
    pub async fn put(&mut self, part: Bytes) -> Result<()> {
        while self.running.len() >= self.max {
            if let Some(r) = self.running.next().await {
                r?;
            }
        }
        self.running.push(self.up.put_part(PutPayload::from(part)));
        Ok(())
    }

    /// Waits for every part, then completes the upload.
    pub async fn complete(&mut self) -> Result<()> {
        while let Some(r) = self.running.next().await {
            r?;
        }
        self.up.complete().await?;
        Ok(())
    }

    /// Drops the parts in flight and aborts the upload; nothing becomes visible.
    pub async fn abort(mut self) {
        self.running.clear();
        let _ = self.up.abort().await;
    }
}

/// Blob objects the handle uploaded with [`crate::Table::upload_blob`], with when each upload
/// began (client monotonic time). A write may commit such a reference without a copy while it
/// is within the commit deadline of that time.
#[derive(Debug, Default)]
pub(crate) struct Staged(Mutex<HashMap<String, Duration>>);

impl Staged {
    pub fn insert(&self, key: String, at: Duration) {
        if let Ok(mut g) = self.0.lock() {
            g.insert(key, at);
        }
    }

    pub fn get(&self, key: &str) -> Option<Duration> {
        self.0.lock().ok().and_then(|g| g.get(key).copied())
    }

    /// Forgets uploads older than `deadline` at `now`: they will be copied anyway.
    pub fn prune(&self, now: Duration, deadline: Duration) {
        if let Ok(mut g) = self.0.lock() {
            g.retain(|_, at| now.saturating_sub(*at) < deadline);
        }
    }
}

/// What a write's blob preparation did, for its commit clock and its report.
#[derive(Debug, Default, Clone)]
pub(crate) struct Prepared {
    /// Values uploaded and references copied.
    pub uploaded: usize,
    pub copied: usize,
    /// The earliest upload of a staged value committed without a copy.
    pub staged_from: Option<Duration>,
    /// Keys of the objects uploaded or copied, recorded before their requests are sent, so a
    /// write that does not commit can delete them (D65).
    pub keys: Vec<String>,
}

/// One value's work.
enum Task {
    /// Upload these bytes.
    Upload(Bytes),
    /// Copy from this key.
    Copy(String),
}

/// The table-side context of [`prepare`].
pub(crate) struct Ctx<'a> {
    pub store: &'a Arc<dyn ObjectStore>,
    pub prefix: &'a Path,
    pub clock: &'a Arc<dyn Clock>,
    pub inline_bytes: u64,
    pub reuse: BlobReuse,
    pub staged: &'a Staged,
    /// Staged uploads younger than this may be committed as they are.
    pub deadline: Duration,
    /// Whether staged uploads may be used at all (not after a restart past the deadline).
    pub use_staged: bool,
    pub concurrency: usize,
}

/// Moves the blob columns of `batch` (named as in `blob_cols`) to their stored form: values
/// above the threshold uploaded, external references not uploaded by this write copied, sizes
/// and checksums of inline values recomputed. Other columns are left as they are.
pub(crate) async fn prepare(
    ctx: &Ctx<'_>,
    batch: &RecordBatch,
    blob_cols: &HashSet<String>,
    out: &mut Prepared,
) -> Result<RecordBatch> {
    if blob_cols.is_empty() {
        return Ok(batch.clone());
    }
    let schema = batch.schema();
    let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
    for (ci, f) in schema.fields().iter().enumerate() {
        if !blob_cols.contains(f.name()) || !is_blob(f.data_type()) {
            continue;
        }
        cols[ci] = prepare_column(ctx, f.name(), batch.column(ci).as_ref(), out).await?;
    }
    Ok(RecordBatch::try_new(schema, cols)?)
}

async fn prepare_column(
    ctx: &Ctx<'_>,
    name: &str,
    a: &dyn Array,
    out: &mut Prepared,
) -> Result<ArrayRef> {
    let p = Parts::of(a)?;
    let mut vals: Vec<Option<BlobRef>> = Vec::with_capacity(a.len());
    // (row, task); copies of one source key are done once.
    let mut tasks: Vec<(usize, Task)> = Vec::new();
    for i in 0..a.len() {
        let Some(mut r) = p.get(i) else {
            vals.push(None);
            continue;
        };
        match (&r.inline, &r.key) {
            (Some(b), None) => {
                r.size = b.len() as u64;
                r.crc32c = crc32c(b);
                if r.size > ctx.inline_bytes {
                    tasks.push((i, Task::Upload(r.inline.take().unwrap_or_default())));
                }
            }
            (None, Some(k)) => {
                if !valid_key(k) {
                    return Err(Error::InvalidBatch(format!(
                        "blob column {name:?}: {k:?} is not a blob key"
                    )));
                }
                let staged = ctx
                    .use_staged
                    .then(|| ctx.staged.get(k))
                    .flatten()
                    .filter(|at| ctx.clock.now().saturating_sub(*at) < ctx.deadline);
                match (staged, ctx.reuse) {
                    (Some(at), _) => {
                        out.staged_from = Some(out.staged_from.map_or(at, |s| s.min(at)));
                    }
                    (None, BlobReuse::TrustCaller) => {}
                    (None, BlobReuse::Copy) => tasks.push((i, Task::Copy(k.clone()))),
                }
            }
            _ => {
                return Err(Error::InvalidBatch(format!(
                    "blob column {name:?}: a value needs exactly one of `inline` and `key`"
                )));
            }
        }
        vals.push(Some(r));
    }
    // One new key per upload and per distinct copied key.
    let mut copy_keys: HashMap<String, String> = HashMap::new();
    let mut jobs: Vec<(Path, Task)> = Vec::new();
    let mut new_keys: Vec<(usize, String)> = Vec::with_capacity(tasks.len());
    for (row, t) in tasks {
        match t {
            Task::Upload(b) => {
                let k = new_key(ctx.clock.as_ref());
                jobs.push((join_key(ctx.prefix, &k), Task::Upload(b)));
                new_keys.push((row, k));
            }
            Task::Copy(from) => {
                let k = match copy_keys.get(&from) {
                    Some(k) => k.clone(),
                    None => {
                        let k = new_key(ctx.clock.as_ref());
                        jobs.push((join_key(ctx.prefix, &k), Task::Copy(from.clone())));
                        copy_keys.insert(from, k.clone());
                        k
                    }
                };
                new_keys.push((row, k));
            }
        }
    }
    let mut seen: HashSet<&str> = HashSet::new();
    out.keys.extend(
        new_keys
            .iter()
            .filter(|(_, k)| seen.insert(k.as_str()))
            .map(|(_, k)| k.clone()),
    );
    let (mut uploaded, mut copied) = (0, 0);
    for (_, t) in &jobs {
        match t {
            Task::Upload(_) => uploaded += 1,
            Task::Copy(_) => copied += 1,
        }
    }
    let store = ctx.store.clone();
    let prefix = ctx.prefix.clone();
    futures::stream::iter(jobs)
        .map(|(to, t)| {
            let (store, prefix) = (store.clone(), prefix.clone());
            async move {
                match t {
                    Task::Upload(b) => put_bytes(&store, &to, b).await,
                    Task::Copy(from) => match store.copy(&join_key(&prefix, &from), &to).await {
                        Ok(()) => Ok(()),
                        Err(object_store::Error::NotFound { .. }) => {
                            Err(Error::BlobNotFound(NotFound::bare(from)))
                        }
                        Err(e) => Err(e.into()),
                    },
                }
            }
        })
        .buffer_unordered(ctx.concurrency.max(1))
        .try_collect::<Vec<()>>()
        .await?;
    out.uploaded += uploaded;
    out.copied += copied;
    if uploaded > 0 {
        fail::fail_point!("moraine::blob::after_upload");
    }
    if copied > 0 {
        fail::fail_point!("moraine::blob::after_copy");
    }
    for (row, k) in new_keys {
        if let Some(Some(r)) = vals.get_mut(row) {
            r.inline = None;
            r.key = Some(k);
        }
    }
    let mut c = BlobColumn::new();
    for v in &vals {
        match v {
            Some(r) => c.push_ref(r),
            None => c.push_null(),
        }
    }
    let arr = c.finish();
    // Keep the column's own field types (nullability of the children may differ).
    Ok(match a.data_type() {
        DataType::Struct(fs) if fs != &children() => {
            let s = arr.as_struct();
            Arc::new(StructArray::try_new(
                fs.clone(),
                s.columns().to_vec(),
                s.nulls().cloned(),
            )?)
        }
        _ => arr,
    })
}

/// The external keys of the blob columns of `batch`.
pub(crate) fn keys_of(batch: &RecordBatch, out: &mut HashSet<String>) -> Result<()> {
    for (i, f) in batch.schema().fields().iter().enumerate() {
        if !is_blob(f.data_type()) {
            continue;
        }
        let p = Parts::of(batch.column(i).as_ref())?;
        for r in 0..batch.num_rows() {
            if let Some(k) = p.key(r) {
                out.insert(k.to_string());
            }
        }
    }
    Ok(())
}

/// The names of the blob columns of `schema`.
pub(crate) fn columns(schema: &arrow::datatypes::Schema) -> HashSet<String> {
    schema
        .fields()
        .iter()
        .filter(|f| is_blob(f.data_type()))
        .map(|f| f.name().clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_valid_and_spread() {
        let c = crate::clock::SystemClock::new();
        let mut dirs = HashSet::new();
        for _ in 0..200 {
            let k = new_key(&c);
            assert!(valid_key(&k), "{k}");
            assert_eq!(&k[17..18], "7", "version nibble of {k}");
            dirs.insert(k[..2].to_string());
        }
        assert!(dirs.len() > 50);
        for bad in [
            "",
            "../x",
            "ab/../../etc",
            "AB/0198c2a0-0000-7000-8000-000000000000",
        ] {
            assert!(!valid_key(bad), "{bad}");
        }
    }

    #[test]
    fn column_round_trip_b5() {
        let mut c = BlobColumn::new();
        c.push_bytes(b"", None);
        c.push_null();
        c.push_bytes(b"abc", Some("text/plain"));
        let a = c.finish();
        assert!(is_blob(a.data_type()));
        let r = refs_of(a.as_ref()).unwrap();
        // Zero length is stored inline with size 0; null is null: distinct on read (case B5).
        let empty = r[0].as_ref().unwrap();
        assert_eq!((empty.size(), empty.is_inline()), (0, true));
        assert!(r[1].is_none());
        assert_eq!(r[2].as_ref().unwrap().crc32c(), crc32c(b"abc"));
        assert_eq!(r[2].as_ref().unwrap().content_type(), Some("text/plain"));
    }
}
