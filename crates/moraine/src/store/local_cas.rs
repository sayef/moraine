//! `PutMode::Update` for the local filesystem (D15, spike item 12).
//!
//! `LocalFileSystem` supports put-if-absent but not compare-and-swap. This wrapper adds it for
//! single-host use (case S11): the writer takes an exclusive lock on the file currently at the
//! path, checks that the path still names that file, compares the ETag, and replaces the file
//! atomically (a staging file renamed into place). The rename gives the
//! path a new inode, so a writer that was waiting for the lock on the old file sees that the
//! path moved on and starts again; its ETag check then fails.
//!
//! The lock is advisory: compare-and-swap is atomic against other `Update` and `Create` writes
//! through this wrapper (in any process on the host), not against plain overwrites. Network
//! filesystems are not supported (their lock semantics vary).

use std::fmt;
use std::fs::File;
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
    UpdateVersion,
};

/// A `LocalFileSystem` with compare-and-swap.
#[derive(Debug, Clone)]
pub struct LocalCasStore {
    inner: Arc<LocalFileSystem>,
}

impl LocalCasStore {
    /// Wraps a local filesystem store, for
    /// [`Database::builder_for_store`](crate::Database::builder_for_store) on a directory.
    pub fn new(inner: Arc<LocalFileSystem>) -> Self {
        LocalCasStore { inner }
    }

    async fn put_update(
        &self,
        location: &Path,
        payload: PutPayload,
        version: UpdateVersion,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let precondition = |msg: &str| object_store::Error::Precondition {
            path: location.to_string(),
            source: msg.to_string().into(),
        };
        if !opts.attributes.is_empty() {
            return Err(object_store::Error::NotImplemented {
                operation: "compare-and-swap with attributes".into(),
                implementer: self.to_string(),
            });
        }
        let Some(expected) = version.e_tag else {
            return Err(precondition("compare-and-swap needs an ETag"));
        };
        let fs_path = self.inner.path_to_filesystem(location)?;
        let r = tokio::task::spawn_blocking(move || swap(&fs_path, &expected, &payload))
            .await
            .map_err(|e| object_store::Error::JoinError { source: e })?;
        match r {
            Ok(Swap::Done(e_tag)) => Ok(PutResult {
                e_tag: Some(e_tag),
                version: None,
            }),
            Ok(Swap::Missing) => Err(precondition("object does not exist")),
            Ok(Swap::Changed) => Err(precondition("ETag does not match")),
            Err(e) => Err(object_store::Error::Generic {
                store: "LocalCasStore",
                source: Box::new(e),
            }),
        }
    }
}

enum Swap {
    Done(String),
    Missing,
    Changed,
}

/// The ETag `LocalFileSystem` reports for a file: inode, modification time in microseconds and
/// size, in hex.
fn local_etag(m: &std::fs::Metadata) -> String {
    let mtime = m
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .unwrap_or_default()
        .as_micros();
    format!("{:x}-{mtime:x}-{:x}", inode(m), m.len())
}

#[cfg(unix)]
fn inode(m: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    m.ino()
}

#[cfg(not(unix))]
fn inode(_m: &std::fs::Metadata) -> u64 {
    0
}

/// Compare-and-swap under the lock of the current file.
///
/// The new file is written next to the target under a staging name (with `#`, which
/// `LocalFileSystem` listings skip) and renamed into place. Its modification time is set to at
/// least one microsecond after the replaced file's: file systems reuse inode numbers and record
/// times at a coarse kernel tick, so without this a later version could get the same
/// (inode, time, size) ETag as an earlier one, and a writer holding the earlier ETag would
/// overwrite a version it never saw (the ABA problem).
fn swap(path: &std::path::Path, expected: &str, payload: &PutPayload) -> std::io::Result<Swap> {
    let Some(locked) = lock_current(path)? else {
        return Ok(Swap::Missing);
    };
    let old = locked.metadata()?;
    if local_etag(&old) != expected {
        return Ok(Swap::Changed);
    }
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let nonce = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let staging = path.with_file_name(format!("{name}#cas-{}-{nonce:x}", std::process::id()));
    let result = (|| {
        let mut tmp = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)?;
        for chunk in payload.iter() {
            tmp.write_all(chunk)?;
        }
        let floor = old.modified()? + Duration::from_micros(1);
        tmp.set_modified(std::time::SystemTime::now().max(floor))?;
        let meta = tmp.metadata()?;
        drop(tmp);
        std::fs::rename(&staging, path)?;
        Ok(Swap::Done(local_etag(&meta)))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&staging);
    }
    drop(locked);
    result
}

/// Opens and exclusively locks the file at `path`, retrying until the locked file is still the
/// one the path names. `None` when nothing is there.
fn lock_current(path: &std::path::Path) -> std::io::Result<Option<File>> {
    loop {
        let f = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        f.lock()?;
        match std::fs::metadata(path) {
            Ok(now) if same_file(&f.metadata()?, &now) => return Ok(Some(f)),
            Ok(_) => continue,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        }
    }
}

#[cfg(unix)]
fn same_file(a: &std::fs::Metadata, b: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    a.dev() == b.dev() && a.ino() == b.ino()
}

#[cfg(not(unix))]
fn same_file(_a: &std::fs::Metadata, _b: &std::fs::Metadata) -> bool {
    // Windows refuses to replace a file another handle has open, so the locked file is
    // always the current one.
    true
}

impl fmt::Display for LocalCasStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalCas({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for LocalCasStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        match &opts.mode {
            PutMode::Update(v) => {
                let v = v.clone();
                self.put_update(location, payload, v, opts).await
            }
            _ => self.inner.put_opts(location, payload, opts).await,
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
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

    /// A copy is a new object with its own modification time, as on the clouds: the local
    /// filesystem's own copy is a hard link, which shares the source's time, and collectors
    /// would take a copy just made for an old object (case B3).
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        use object_store::ObjectStoreExt;
        let body = self.inner.get(from).await?.bytes().await?;
        let mode = match options.mode {
            object_store::CopyMode::Overwrite => PutMode::Overwrite,
            object_store::CopyMode::Create => PutMode::Create,
        };
        self.inner
            .put_opts(
                to,
                PutPayload::from(body),
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await
            .map(drop)
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;

    fn update(e_tag: Option<String>) -> PutOptions {
        PutOptions {
            mode: PutMode::Update(UpdateVersion {
                e_tag,
                version: None,
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn stale_etag_and_missing_object_fail() {
        let dir = tempfile::tempdir().unwrap();
        let s = LocalCasStore::new(Arc::new(
            LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
        ));
        let p = Path::from("lease.json");
        let r = s.put_opts(&p, "a".into(), update(Some("x".into()))).await;
        assert!(matches!(r, Err(object_store::Error::Precondition { .. })));
        let first = s.put(&p, "a".into()).await.unwrap();
        let second = s
            .put_opts(&p, "b".into(), update(first.e_tag.clone()))
            .await
            .unwrap();
        assert_ne!(first.e_tag, second.e_tag);
        let r = s.put_opts(&p, "c".into(), update(first.e_tag)).await;
        assert!(matches!(r, Err(object_store::Error::Precondition { .. })));
        let body = s.get(&p).await.unwrap().bytes().await.unwrap();
        assert_eq!(&body[..], b"b");
    }

    /// 16 tasks increment one counter by compare-and-swap; no increment is lost.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_increments_are_not_lost() {
        let dir = tempfile::tempdir().unwrap();
        let s = Arc::new(LocalCasStore::new(Arc::new(
            LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
        )));
        let p = Path::from("counter");
        s.put(&p, "0".into()).await.unwrap();
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let s = s.clone();
            let p = p.clone();
            tasks.push(tokio::spawn(async move {
                for _ in 0..25 {
                    loop {
                        let r = s.get(&p).await.unwrap();
                        let e_tag = r.meta.e_tag.clone();
                        let n: u64 = std::str::from_utf8(&r.bytes().await.unwrap())
                            .unwrap()
                            .parse()
                            .unwrap();
                        let next = PutPayload::from((n + 1).to_string());
                        match s.put_opts(&p, next, update(e_tag)).await {
                            Ok(_) => break,
                            Err(object_store::Error::Precondition { .. }) => continue,
                            Err(e) => panic!("{e}"),
                        }
                    }
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let body = s.get(&p).await.unwrap().bytes().await.unwrap();
        assert_eq!(&body[..], b"400");
    }
}
