//! Capability probe for conditional writes (case S2).
//!
//! Some S3-compatible services accept `If-None-Match: *` and `If-Match` and then ignore them.
//! Delta commits on such a store would silently overwrite each other, so a writer checks once
//! per store and process, and refuses to write when the store does not honour both modes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};

use super::{Class, classify};
use crate::clock::{Clock, random_name};
use crate::error::{Error, Result};

/// How a store handles one conditional write mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Support {
    /// The condition is checked.
    Honoured,
    /// The write succeeds although the condition is false.
    Ignored,
    /// The store refuses the mode (not implemented or not supported).
    Rejected,
}

/// Result of [`probe_conditional_writes`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Capabilities {
    /// `PutMode::Create` (put-if-absent): needed for every commit.
    pub create: Support,
    /// `PutMode::Update` (compare-and-swap on the ETag): needed for leases.
    pub update: Support,
}

impl Capabilities {
    /// Whether both conditional writes are honoured, as opening a store for writing requires.
    pub fn can_write(&self) -> bool {
        self.create == Support::Honoured && self.update == Support::Honoured
    }
}

/// Results cached per store and prefix for the life of the process.
fn cache() -> &'static Mutex<HashMap<String, Capabilities>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Capabilities>>> = OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// Probes `store` once per process (cached by the store's display name and `prefix`) and returns
/// an error unless both conditional modes are honoured.
pub async fn require_conditional_writes(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    clock: &dyn Clock,
) -> Result<Capabilities> {
    let key = format!("{store}|{prefix}");
    let cached = cache().lock().map(|c| c.get(&key).copied()).ok().flatten();
    let caps = match cached {
        Some(c) => c,
        None => {
            let c = probe_conditional_writes(store.as_ref(), prefix, clock).await?;
            if let Ok(mut m) = cache().lock() {
                m.insert(key, c);
            }
            c
        }
    };
    if caps.can_write() {
        Ok(caps)
    } else {
        Err(Error::ConditionalWritesUnsupported(format!(
            "put-if-absent {:?}, compare-and-swap {:?}",
            caps.create, caps.update
        )))
    }
}

/// Writes probe objects under `<prefix>/probe/` and reports what the store did with each mode.
/// Probe objects are deleted afterwards (a failed delete is ignored; they are tiny).
///
/// Uncached; [`require_conditional_writes`] caches.
pub async fn probe_conditional_writes(
    store: &dyn ObjectStore,
    prefix: &Path,
    clock: &dyn Clock,
) -> Result<Capabilities> {
    // An ambiguous outcome makes one round inconclusive; start again with fresh names.
    let mut last = None;
    for _ in 0..3 {
        match probe_once(store, prefix, clock).await {
            Ok(c) => return Ok(c),
            Err(e) if is_ambiguous(&e) => last = Some(e),
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap_or_else(|| Error::Internal("probe did not run".into())))
}

fn is_ambiguous(e: &Error) -> bool {
    matches!(e, Error::Storage(s) if classify(s.inner()) == Class::Ambiguous)
}

async fn probe_once(
    store: &dyn ObjectStore,
    prefix: &Path,
    clock: &dyn Clock,
) -> Result<Capabilities> {
    let create_path = prefix.clone().join("probe").join(random_name(clock));
    let update_path = prefix.clone().join("probe").join(random_name(clock));
    let create = probe_create(store, &create_path).await;
    let update = probe_update(store, &update_path).await;
    for p in [&create_path, &update_path] {
        let _ = store.delete(p).await;
    }
    Ok(Capabilities {
        create: create?,
        update: update?,
    })
}

fn put_mode(mode: PutMode) -> PutOptions {
    PutOptions {
        mode,
        ..Default::default()
    }
}

fn rejected(e: &object_store::Error) -> bool {
    matches!(
        e,
        object_store::Error::NotImplemented { .. } | object_store::Error::NotSupported { .. }
    )
}

async fn probe_create(store: &dyn ObjectStore, path: &Path) -> Result<Support> {
    match store
        .put_opts(
            path,
            PutPayload::from_static(b"1"),
            put_mode(PutMode::Create),
        )
        .await
    {
        Ok(_) => {}
        Err(e) if rejected(&e) => return Ok(Support::Rejected),
        Err(e) => return Err(e.into()),
    }
    match store
        .put_opts(
            path,
            PutPayload::from_static(b"2"),
            put_mode(PutMode::Create),
        )
        .await
    {
        Ok(_) => Ok(Support::Ignored),
        Err(object_store::Error::AlreadyExists { .. }) => Ok(Support::Honoured),
        Err(e) if rejected(&e) => Ok(Support::Rejected),
        Err(e) => Err(e.into()),
    }
}

async fn probe_update(store: &dyn ObjectStore, path: &Path) -> Result<Support> {
    let first = store
        .put_opts(
            path,
            PutPayload::from_static(b"1"),
            put_mode(PutMode::Overwrite),
        )
        .await?;
    let stale = UpdateVersion {
        e_tag: Some("\"moraine-probe-stale-etag\"".into()),
        version: None,
    };
    match store
        .put_opts(
            path,
            PutPayload::from_static(b"2"),
            put_mode(PutMode::Update(stale)),
        )
        .await
    {
        Ok(_) => return Ok(Support::Ignored),
        Err(object_store::Error::Precondition { .. }) => {}
        Err(e) if rejected(&e) => return Ok(Support::Rejected),
        Err(e) => return Err(e.into()),
    }
    let current = UpdateVersion {
        e_tag: first.e_tag,
        version: first.version,
    };
    match store
        .put_opts(
            path,
            PutPayload::from_static(b"3"),
            put_mode(PutMode::Update(current)),
        )
        .await
    {
        Ok(_) => Ok(Support::Honoured),
        // The store refused a correct ETag: compare-and-swap cannot work on it.
        Err(object_store::Error::Precondition { .. }) => Ok(Support::Rejected),
        Err(e) if rejected(&e) => Ok(Support::Rejected),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::SystemClock;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn memory_honours_both() {
        let s = InMemory::new();
        let c = probe_conditional_writes(&s, &Path::from("meta"), &SystemClock::new())
            .await
            .unwrap();
        assert_eq!(c.create, Support::Honoured);
        assert_eq!(c.update, Support::Honoured);
        assert!(
            s.list(None).next().await.is_none(),
            "probe objects are removed"
        );
    }

    #[tokio::test]
    async fn local_needs_the_cas_wrapper() {
        let dir = tempfile::tempdir().unwrap();
        let fs = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let clock = SystemClock::new();
        let raw = probe_conditional_writes(fs.as_ref(), &Path::from("meta"), &clock)
            .await
            .unwrap();
        assert_eq!(raw.create, Support::Honoured);
        assert_eq!(raw.update, Support::Rejected);
        let wrapped: Arc<dyn ObjectStore> = Arc::new(crate::store::LocalCasStore::new(fs));
        let c = require_conditional_writes(&wrapped, &Path::from("meta"), &clock)
            .await
            .unwrap();
        assert!(c.can_write());
    }

    use futures::StreamExt;
}
