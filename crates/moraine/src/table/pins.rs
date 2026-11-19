//! Named pins (D39, D49): versions kept readable through vacuum.
//!
//! A pin is a table property, `moraine.pins` (name to version), committed like an index change,
//! plus a manifest object, `meta/pins/<name>/<version>.json`, written before that commit. The
//! manifest is the pinned version's metadata and file list in the log-entry format, so a pinned
//! version is read without the log (which log cleanup shortens) and vacuum keeps exactly the
//! files the manifests of the current pins name. There is no general time travel: only what
//! someone pinned stays readable once the grace period has passed.

use std::collections::BTreeMap;
use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};

use super::index::Change;
use super::{Table, TableInner, TableSnapshot};
use crate::error::{Error, Result};
use crate::log::{Loaded, NewEntry};
use crate::names::NotFound;
use crate::snapshot::Snapshot;

/// The table property holding the pins: a JSON object from name to version.
pub(crate) const PINS_PROPERTY: &str = "moraine.pins";

/// The table property recording the newest purge (D66): the version the purge followed.
/// Files that log entries up to it removed may be gone at once, so no older version can be
/// pinned, and checkpoints older than it are stale.
pub(crate) const PURGED_PROPERTY: &str = "moraine.purged";

/// The version of the newest purge in table properties `config`, if any.
pub(crate) fn purged(config: &BTreeMap<String, String>) -> Result<Option<u64>> {
    match config.get(PURGED_PROPERTY) {
        None => Ok(None),
        Some(v) => v.parse().map(Some).map_err(|e| {
            Error::corrupt(
                format!("table property {PURGED_PROPERTY}"),
                format_args!("{v}: {e}"),
            )
        }),
    }
}

/// The pins in table properties `config`.
pub(crate) fn parse_pins(config: &BTreeMap<String, String>) -> Result<BTreeMap<String, u64>> {
    match config.get(PINS_PROPERTY) {
        None => Ok(BTreeMap::new()),
        Some(v) => serde_json::from_str(v).map_err(|e| {
            Error::corrupt(
                format!("table property {PINS_PROPERTY}"),
                format_args!("{v}: {e}"),
            )
        }),
    }
}

impl Table {
    /// Pins `version` under `name`: vacuum keeps its files, and
    /// [`Table::snapshot_at_pin`] reads it, until [`Table::unpin`]. Pinning a name again moves
    /// it. The version must still be readable: the current one, or an older one whose files
    /// were replaced less than three quarters of the grace period ago (D49); otherwise
    /// [`Error::VersionNotAvailable`].
    ///
    /// ```no_run
    /// # async fn f(t: moraine::Table) -> moraine::Result<()> {
    /// let v = t.refresh().await?;
    /// t.pin("release-1", v).await?;
    /// // ... later writes, compaction and vacuum ...
    /// let old = t.snapshot_at_pin("release-1").await?;
    /// assert_eq!(old.version(), v);
    /// let rows = old.scan().collect().await?;
    /// t.unpin("release-1").await?;
    /// # let _ = rows; Ok(()) }
    /// ```
    pub async fn pin(&self, name: &str, version: u64) -> Result<()> {
        validate_pin_name(name)?;
        let snap = self.inner.snapshot_at(version).await?;
        let body = self.inner.layer.encode(
            &NewEntry {
                meta: Some(snap.meta.as_ref().clone()),
                protocol: false,
                adds: snap.files.iter().map(|f| f.as_ref().clone()).collect(),
                removes: Vec::new(),
                txn: None,
                caller_txns: Vec::new(),
                operation: "PIN",
                timestamp: self.inner.db.clock().wall_now().timestamp_millis(),
                data_change: false,
            },
            snap.schema(),
        )?;
        self.inner.db.require_writable().await?;
        let store = self.inner.db.store.object_store();
        store
            .put(
                &self.inner.pin_manifest(name, version),
                PutPayload::from(body),
            )
            .await?;
        match self
            .inner
            .change_indexes(Change::Pin(name.into(), Some(version)))
            .await
        {
            Ok(_) => Ok(()),
            Err(Error::VersionNotAvailable(v)) => {
                // Refused because a purge followed: no pin of this version can commit any
                // more, so unless one of this name already holds it, its manifest is nobody's
                // (D66). The refusal read the newest version, which the handle now holds.
                let cur = self.inner.current();
                let live = parse_pins(&cur.meta.configuration)?.get(name) == Some(&version);
                if !live && purged(&cur.meta.configuration)?.is_some_and(|p| v < p) {
                    let _ = store.delete(&self.inner.pin_manifest(name, version)).await;
                }
                Err(Error::VersionNotAvailable(v))
            }
            Err(e) => Err(e),
        }
    }

    /// Removes the pin `name`; vacuum collects the files only it kept after the grace period.
    /// Removing a pin that does not exist changes nothing.
    pub async fn unpin(&self, name: &str) -> Result<()> {
        validate_pin_name(name)?;
        self.inner
            .change_indexes(Change::Pin(name.into(), None))
            .await?;
        Ok(())
    }

    /// The pins of the snapshot in hand, by name (refresh first for the newest).
    pub fn pins(&self) -> Result<BTreeMap<String, u64>> {
        parse_pins(&self.inner.current().meta.configuration)
    }

    /// The pinned version `name`, for reading. [`Error::PinNotFound`] when the snapshot in hand
    /// has no such pin.
    pub async fn snapshot_at_pin(&self, name: &str) -> Result<TableSnapshot> {
        let pins = self.pins()?;
        let version = *pins
            .get(name)
            .ok_or_else(|| Error::PinNotFound(NotFound::among(name, pins.keys().cloned())))?;
        let snap = self.inner.pinned(name, version).await?;
        Ok(TableSnapshot::new(self.inner.clone(), snap))
    }
}

fn validate_pin_name(name: &str) -> Result<()> {
    crate::index::spec::validate_index_name(name).map_err(|_| {
        Error::InvalidQuery(format!(
            "pin name {name:?} does not match [a-z0-9][a-z0-9_-]{{0,63}}"
        ))
    })
}

impl TableInner {
    /// `<db>/<table>/meta`: leases, pins, the inbox and server-time probes.
    pub(crate) fn meta_prefix(&self) -> Path {
        self.db.root.clone().join(self.name.as_str()).join("meta")
    }

    pub(crate) fn pin_manifest(&self, name: &str, version: u64) -> Path {
        self.meta_prefix()
            .join("pins")
            .join(name)
            .join(format!("{version:020}.json"))
    }

    /// The snapshot at `version`, rebuilt from the log when it is not the one in hand, and
    /// still safe from vacuum for at least a commit deadline (see [`Table::pin`]).
    pub(crate) async fn snapshot_at(self: &Arc<Self>, version: u64) -> Result<Arc<Snapshot>> {
        let mut cur = self.current();
        if version > cur.version {
            cur = self.refresh().await?;
        }
        if version == cur.version {
            return Ok(cur);
        }
        if version > cur.version {
            return Err(Error::VersionNotAvailable(version));
        }
        if purged(&cur.meta.configuration)?.is_some_and(|p| version < p) {
            return Err(Error::VersionNotAvailable(version));
        }
        // Files of `version` can only have been removed from the next version on, so that
        // entry's age bounds how long vacuum has been allowed to wait for them.
        let Some((_, at)) = self.layer.read_entry_at(version + 1, None).await? else {
            return Err(Error::VersionNotAvailable(version));
        };
        let now = crate::store::server_now(
            self.db.store.object_store().as_ref(),
            &self.meta_prefix(),
            self.db.clock().as_ref(),
        )
        .await?
        .server;
        let grace = cur.def.grace_period;
        let limit = grace - cur.def.commit_deadline();
        let age = (now - at).to_std().unwrap_or_default();
        if age > limit {
            return Err(Error::VersionNotAvailable(version));
        }
        let loaded = self.layer.load_at(version).await?;
        Ok(Arc::new(Snapshot::from_loaded(
            loaded,
            self.db.clock().now(),
        )?))
    }

    /// The pinned version `name` at `version`, from its manifest (cached per handle).
    pub(crate) async fn pinned(&self, name: &str, version: u64) -> Result<Arc<Snapshot>> {
        let key = (name.to_string(), version);
        if let Some(s) = self.pinned.lock().ok().and_then(|g| g.get(&key).cloned()) {
            return Ok(s);
        }
        let entry = self.read_pin_manifest(name, version).await?;
        let meta = entry
            .meta
            .clone()
            .ok_or_else(|| Error::corrupt(format!("pin {name}"), "manifest without metadata"))?;
        let snap = Arc::new(Snapshot::from_loaded(
            Loaded {
                base: Some((version, meta, entry.adds)),
                base_txns: Vec::new(),
                base_txn_times: Vec::new(),
                entries: Vec::new(),
            },
            self.db.clock().now(),
        )?);
        if let Ok(mut g) = self.pinned.lock() {
            g.insert(key, snap.clone());
        }
        Ok(snap)
    }

    /// The manifest of pin `name` at `version`, parsed as a log entry.
    pub(crate) async fn read_pin_manifest(
        &self,
        name: &str,
        version: u64,
    ) -> Result<crate::log::LogEntry> {
        let store = self.db.store.object_store();
        let body = match store.get(&self.pin_manifest(name, version)).await {
            Ok(r) => r.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(Error::corrupt(
                    format!("pin {name}"),
                    format_args!("the manifest of version {version} is missing"),
                ));
            }
            Err(e) => return Err(e.into()),
        };
        self.layer.parse_entry(version, &body)
    }
}
