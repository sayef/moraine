//! An in-memory object store on simulated time.
//!
//! Like `object_store::memory::InMemory`, with three differences that the simulation needs:
//! last-modified times come from the [`SimClock`] (so tests can move past a grace period), the
//! contents are in ordered maps (so listings and every decision depend on the seed only), and
//! [`SimStore::snapshot`] exposes the contents for invariant checks.
//!
//! [`SimStore::with_mtime_resolution`] truncates last-modified times as real stores do (whole
//! seconds on S3), so they lag the server's clock by up to that resolution.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyMode, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result, UploadPart,
};

use crate::clock::SimClock;

#[derive(Clone, Debug)]
struct Entry {
    data: Bytes,
    e_tag: u64,
    last_modified: DateTime<Utc>,
}

#[derive(Default)]
struct Inner {
    objects: BTreeMap<String, Entry>,
    next_etag: u64,
    /// Every object ever written, deleted or not, for the collectors' checks.
    archive: BTreeMap<String, Vec<Archived>>,
}

/// One write of an object, kept after the object is deleted or overwritten.
#[derive(Clone, Debug)]
pub struct Archived {
    pub data: Bytes,
    pub created: DateTime<Utc>,
    /// When it was deleted or overwritten; `None` while it is the current object.
    pub gone: Option<DateTime<Utc>>,
    /// Whether it was deleted (rather than overwritten).
    pub deleted: bool,
}

/// See the module documentation.
#[derive(Clone)]
pub struct SimStore {
    inner: Arc<Mutex<Inner>>,
    clock: SimClock,
    mtime_resolution: std::time::Duration,
}

impl fmt::Debug for SimStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimStore")
    }
}

impl fmt::Display for SimStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimStore")
    }
}

fn etag(n: u64) -> String {
    format!("\"{n}\"")
}

impl SimStore {
    pub fn new(clock: SimClock) -> Self {
        SimStore {
            inner: Arc::default(),
            clock,
            mtime_resolution: std::time::Duration::ZERO,
        }
    }

    /// Last-modified times truncated to multiples of `resolution` since the simulation's
    /// epoch (one second for S3). Zero, the default, keeps them exact.
    pub fn with_mtime_resolution(mut self, resolution: std::time::Duration) -> Self {
        self.mtime_resolution = resolution;
        self
    }

    /// The last-modified time the store records for a write at `now`.
    fn stamp(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        let r = self.mtime_resolution.as_nanos();
        if r == 0 {
            return now;
        }
        let since = self.clock.now().as_nanos();
        let lag = since % r;
        now - chrono::Duration::nanoseconds(lag as i64)
    }

    pub fn clock(&self) -> &SimClock {
        &self.clock
    }

    /// Every object with its bytes, ordered by path.
    pub fn snapshot(&self) -> BTreeMap<String, Bytes> {
        let g = self.inner.lock().expect("store lock");
        g.objects
            .iter()
            .map(|(k, v)| (k.clone(), v.data.clone()))
            .collect()
    }

    /// Every write of every object, by path, in order, including deleted ones.
    pub fn archive(&self) -> BTreeMap<String, Vec<Archived>> {
        self.inner.lock().expect("store lock").archive.clone()
    }

    /// The bytes last written at `path`, even if the object was deleted since.
    pub fn peek_archived(&self, path: &str) -> Option<Bytes> {
        let g = self.inner.lock().expect("store lock");
        g.archive
            .get(path)
            .and_then(|v| v.last())
            .map(|a| a.data.clone())
    }

    /// The bytes at `path`, if any, without going through the store API (and so without a
    /// gate or a fault).
    pub fn peek(&self, path: &str) -> Option<Bytes> {
        let g = self.inner.lock().expect("store lock");
        g.objects.get(path).map(|e| e.data.clone())
    }

    fn meta(path: &str, e: &Entry) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            last_modified: e.last_modified,
            size: e.data.len() as u64,
            e_tag: Some(etag(e.e_tag)),
            version: None,
        }
    }

    fn write(&self, path: &Path, data: Bytes, mode: &PutMode) -> Result<PutResult> {
        let key = path.to_string();
        let mut g = self.inner.lock().expect("store lock");
        match mode {
            PutMode::Overwrite => {}
            PutMode::Create => {
                if g.objects.contains_key(&key) {
                    return Err(object_store::Error::AlreadyExists {
                        path: key,
                        source: "exists".into(),
                    });
                }
            }
            PutMode::Update(v) => {
                let current = g.objects.get(&key).map(|e| etag(e.e_tag));
                if current.is_none() || current != v.e_tag {
                    return Err(object_store::Error::Precondition {
                        path: key,
                        source: format!("expected {:?}, found {:?}", v.e_tag, current).into(),
                    });
                }
            }
        }
        g.next_etag += 1;
        let n = g.next_etag;
        let now = self.clock.server_time();
        let versions = g.archive.entry(key.clone()).or_default();
        if let Some(last) = versions.last_mut()
            && last.gone.is_none()
        {
            last.gone = Some(now);
        }
        versions.push(Archived {
            data: data.clone(),
            created: now,
            gone: None,
            deleted: false,
        });
        g.objects.insert(
            key,
            Entry {
                data,
                e_tag: n,
                last_modified: self.stamp(now),
            },
        );
        Ok(PutResult {
            e_tag: Some(etag(n)),
            version: None,
        })
    }

    fn not_found(path: &Path) -> object_store::Error {
        object_store::Error::NotFound {
            path: path.to_string(),
            source: "not found".into(),
        }
    }
}

fn concat(payload: &PutPayload) -> Bytes {
    let mut b = BytesMut::with_capacity(payload.content_length());
    for chunk in payload.iter() {
        b.extend_from_slice(chunk);
    }
    b.freeze()
}

#[async_trait]
impl ObjectStore for SimStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.write(location, concat(&payload), &opts.mode)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(SimUpload {
            store: self.clone(),
            location: location.clone(),
            parts: Arc::default(),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let key = location.to_string();
        let entry = {
            let g = self.inner.lock().expect("store lock");
            g.objects.get(&key).cloned()
        };
        let Some(e) = entry else {
            return Err(Self::not_found(location));
        };
        let meta = Self::meta(&key, &e);
        options.check_preconditions(&meta)?;
        let len = e.data.len() as u64;
        let range = match &options.range {
            Some(r) => r
                .as_range(len)
                .map_err(|err| object_store::Error::Generic {
                    store: "SimStore",
                    source: Box::new(err),
                })?,
            None => 0..len,
        };
        let body = if options.head {
            Bytes::new()
        } else {
            e.data.slice(range.start as usize..range.end as usize)
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(
                futures::stream::once(async move { Ok(body) }).boxed(),
            ),
            meta,
            range,
            attributes: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let inner = self.inner.clone();
        let clock = self.clock.clone();
        locations
            .map(move |p| {
                let p = p?;
                let mut g = inner.lock().expect("store lock");
                if g.objects.remove(&p.to_string()).is_some()
                    && let Some(last) = g.archive.get_mut(&p.to_string()).and_then(|v| v.last_mut())
                {
                    last.gone = Some(clock.server_time());
                    last.deleted = true;
                }
                Ok(p)
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let g = self.inner.lock().expect("store lock");
        let items: Vec<Result<ObjectMeta>> = g
            .objects
            .iter()
            .filter(|(k, _)| prefix.is_none_or(|p| Path::from(k.as_str()).prefix_matches(p)))
            .map(|(k, e)| Ok(Self::meta(k, e)))
            .collect();
        futures::stream::iter(items).boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let g = self.inner.lock().expect("store lock");
        let depth = prefix.map_or(0, |p| p.parts().count());
        let mut common = std::collections::BTreeSet::new();
        let mut objects = Vec::new();
        for (k, e) in &g.objects {
            let path = Path::from(k.as_str());
            if !prefix.is_none_or(|p| path.prefix_matches(p)) {
                continue;
            }
            let parts: Vec<_> = path.parts().collect();
            if parts.len() == depth + 1 {
                objects.push(Self::meta(k, e));
            } else if parts.len() > depth + 1 {
                common.insert(Path::from_iter(parts[..=depth].iter().cloned()));
            }
        }
        Ok(ListResult {
            common_prefixes: common.into_iter().collect(),
            objects,
        })
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let data = {
            let g = self.inner.lock().expect("store lock");
            g.objects.get(&from.to_string()).map(|e| e.data.clone())
        };
        let Some(data) = data else {
            return Err(Self::not_found(from));
        };
        let mode = match options.mode {
            CopyMode::Overwrite => PutMode::Overwrite,
            CopyMode::Create => PutMode::Create,
        };
        self.write(to, data, &mode).map(|_| ())
    }
}

#[derive(Debug)]
struct SimUpload {
    store: SimStore,
    location: Path,
    parts: Arc<Mutex<Vec<Bytes>>>,
}

#[async_trait]
impl MultipartUpload for SimUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.parts.lock().expect("parts lock").push(concat(&data));
        Box::pin(async { Ok(()) })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let parts = std::mem::take(&mut *self.parts.lock().expect("parts lock"));
        let mut b = BytesMut::new();
        for p in parts {
            b.extend_from_slice(&p);
        }
        self.store
            .write(&self.location, b.freeze(), &PutMode::Overwrite)
    }

    async fn abort(&mut self) -> Result<()> {
        self.parts.lock().expect("parts lock").clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{ObjectStoreExt, UpdateVersion};
    use std::time::Duration;

    #[tokio::test]
    async fn conditional_modes_and_sim_time() {
        let clock = SimClock::new();
        let s = SimStore::new(clock.clone());
        let p = Path::from("a/b.json");
        let create = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        let r1 = s.put_opts(&p, "1".into(), create.clone()).await.unwrap();
        assert!(matches!(
            s.put_opts(&p, "2".into(), create).await,
            Err(object_store::Error::AlreadyExists { .. })
        ));
        clock.advance(Duration::from_secs(3600));
        let upd = |e: Option<String>| PutOptions {
            mode: PutMode::Update(UpdateVersion {
                e_tag: e,
                version: None,
            }),
            ..Default::default()
        };
        s.put_opts(&p, "3".into(), upd(r1.e_tag.clone()))
            .await
            .unwrap();
        assert!(matches!(
            s.put_opts(&p, "4".into(), upd(r1.e_tag)).await,
            Err(object_store::Error::Precondition { .. })
        ));
        let m = s.head(&p).await.unwrap();
        assert_eq!(
            m.last_modified,
            crate::clock::sim_epoch() + chrono::Duration::hours(1)
        );
        assert_eq!(&s.get(&p).await.unwrap().bytes().await.unwrap()[..], b"3");
        let l = s.list_with_delimiter(Some(&Path::from("a"))).await.unwrap();
        assert_eq!(l.objects.len(), 1);
        s.put(&Path::from("a/c/d"), "x".into()).await.unwrap();
        let l = s.list_with_delimiter(Some(&Path::from("a"))).await.unwrap();
        assert_eq!(l.common_prefixes, vec![Path::from("a/c")]);
    }
}
