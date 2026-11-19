//! Capability probe for conditional writes (case S2).
//!
//! Some S3-compatible services accept `If-None-Match: *` and `If-Match` and then ignore them.
//! Delta commits on such a store would silently overwrite each other, so a writer checks once
//! per store and process, and refuses to write when the store does not honour both modes.
//!
//! A probe request can be sent twice (object_store resends a request after a 5xx, whatever its
//! mode) or lose its response, and then the answer describes the probe's own earlier write, not
//! the store. Every request therefore writes its own content, and an answer that a resend or a
//! lost response could explain is checked by reading the object back, as the commit path looks
//! up its transaction (D92). Only a verified answer gives a verdict; an outcome that stays
//! unknown fails the probe without one, and nothing is cached.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};

use bytes::Bytes;

use super::{Class, StoreFault, classify};
use crate::clock::{Clock, random_name};
use crate::commit::AttemptError;
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
    // Every request writes its own content, so a read-back tells which request an object holds.
    let nonce = random_name(clock);
    let create = probe_create(store, &create_path, &nonce).await;
    let update = probe_update(store, &update_path, &nonce).await;
    for p in [&create_path, &update_path] {
        let _ = store.delete(p).await;
    }
    Ok(Capabilities {
        create: create?,
        update: update?,
    })
}

/// Sends of one probe request whose outcome stays unknown before the round is abandoned.
const STEP_ATTEMPTS: u32 = 3;

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

/// The payload of probe request `step`.
fn token(nonce: &str, step: &str) -> Bytes {
    Bytes::from(format!("moraine-probe {nonce} {step}"))
}

/// An outcome the probe could not resolve. It is ambiguous, so the round starts again with fresh
/// names, and if every round ends this way the probe fails without a verdict.
fn inconclusive(detail: impl std::fmt::Display) -> Error {
    StoreFault::new(
        Class::Ambiguous,
        format!("capability probe inconclusive: {detail}"),
    )
    .into_store_error()
    .into()
}

/// Whether a failed probe write may still have taken effect: the commit path's rule
/// ([`AttemptError::from_store_is_open`]).
fn open(e: &object_store::Error) -> bool {
    AttemptError::from_store_is_open(e)
}

/// The content and version of the object at `path`, `None` when there is none.
async fn read_back(store: &dyn ObjectStore, path: &Path) -> Result<Option<(Bytes, UpdateVersion)>> {
    match store.get(path).await {
        Ok(r) => {
            let version = UpdateVersion {
                e_tag: r.meta.e_tag.clone(),
                version: r.meta.version.clone(),
            };
            Ok(Some((r.bytes().await?, version)))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

async fn put(store: &dyn ObjectStore, path: &Path, body: &Bytes, mode: PutMode) -> PutRes {
    store
        .put_opts(path, PutPayload::from(body.clone()), put_mode(mode))
        .await
}

type PutRes = std::result::Result<object_store::PutResult, object_store::Error>;

/// Put-if-absent: create the object, then create it again. Only a second create that the store
/// accepted, or a read-back holding the second create's content, says the mode is ignored; an
/// answer that a resent or lost request may explain is checked against the object first.
async fn probe_create(store: &dyn ObjectStore, path: &Path, nonce: &str) -> Result<Support> {
    let first = token(nonce, "create-1");
    let second = token(nonce, "create-2");
    match put(store, path, &first, PutMode::Create).await {
        Ok(_) => {}
        Err(e) if rejected(&e) => return Ok(Support::Rejected),
        // A resent create meets its own first send; a lost response may hide either outcome.
        Err(e) if matches!(e, object_store::Error::AlreadyExists { .. }) || open(&e) => {
            match read_back(store, path).await? {
                Some((body, _)) if body == first => {}
                _ => return Err(inconclusive(e)),
            }
        }
        Err(e) => return Err(e.into()),
    }
    let mut last = None;
    for _ in 0..STEP_ATTEMPTS {
        let e = match put(store, path, &second, PutMode::Create).await {
            Ok(_) => return Ok(Support::Ignored),
            Err(e) if rejected(&e) => return Ok(Support::Rejected),
            Err(e) if matches!(e, object_store::Error::AlreadyExists { .. }) || open(&e) => e,
            Err(e) => return Err(e.into()),
        };
        match read_back(store, path).await? {
            Some((body, _)) if body == second => return Ok(Support::Ignored),
            Some((body, _)) if body == first => {
                if matches!(e, object_store::Error::AlreadyExists { .. }) {
                    return Ok(Support::Honoured);
                }
                // The request may not have reached the store: send it again.
                last = Some(e);
            }
            _ => return Err(inconclusive(e)),
        }
    }
    Err(inconclusive(
        last.map_or_else(String::new, |e| e.to_string()),
    ))
}

/// Compare-and-swap: overwrite the object, update it with a stale ETag (must fail), then with
/// the current one (must succeed). A refusal of the current ETag counts as `Rejected` only once
/// a read-back shows the object unchanged at exactly that ETag; when the object holds the swap's
/// own content, the swap took effect and a resend or a lost response produced the error.
async fn probe_update(store: &dyn ObjectStore, path: &Path, nonce: &str) -> Result<Support> {
    let base = token(nonce, "update-1");
    let stale_body = token(nonce, "update-2");
    let swap = token(nonce, "update-3");
    let written = put(store, path, &base, PutMode::Overwrite).await?;
    let stale = UpdateVersion {
        e_tag: Some("\"moraine-probe-stale-etag\"".into()),
        version: None,
    };
    let mut last = None;
    let mut refused = false;
    for _ in 0..STEP_ATTEMPTS {
        let e = match put(store, path, &stale_body, PutMode::Update(stale.clone())).await {
            Ok(_) => return Ok(Support::Ignored),
            Err(object_store::Error::Precondition { .. }) => {
                refused = true;
                break;
            }
            Err(e) if rejected(&e) => return Ok(Support::Rejected),
            Err(e) if open(&e) => e,
            Err(e) => return Err(e.into()),
        };
        match read_back(store, path).await? {
            Some((body, _)) if body == stale_body => return Ok(Support::Ignored),
            Some((body, _)) if body == base => last = Some(e),
            _ => return Err(inconclusive(e)),
        }
    }
    if !refused {
        return Err(inconclusive(
            last.map_or_else(String::new, |e| e.to_string()),
        ));
    }
    let mut expected = UpdateVersion {
        e_tag: written.e_tag,
        version: written.version,
    };
    let mut last = None;
    for _ in 0..STEP_ATTEMPTS {
        let e = match put(store, path, &swap, PutMode::Update(expected.clone())).await {
            Ok(_) => return Ok(Support::Honoured),
            Err(e) if rejected(&e) => return Ok(Support::Rejected),
            Err(e) if matches!(e, object_store::Error::Precondition { .. }) || open(&e) => e,
            Err(e) => return Err(e.into()),
        };
        match read_back(store, path).await? {
            // The swap took effect: its resend, or the lost response, made the error.
            Some((body, _)) if body == swap => return Ok(Support::Honoured),
            // A stale-ETag update took effect after all.
            Some((body, _)) if body == stale_body => return Ok(Support::Ignored),
            Some((body, now)) if body == base => {
                if matches!(e, object_store::Error::Precondition { .. }) && now == expected {
                    // The store refused the very ETag it reports for the unchanged object.
                    return Ok(Support::Rejected);
                }
                // Not sent, or sent with an ETag the store no longer reports: try again with
                // the one it reports now.
                expected = now;
                last = Some(e);
            }
            _ => return Err(inconclusive(e)),
        }
    }
    Err(inconclusive(
        last.map_or_else(String::new, |e| e.to_string()),
    ))
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
