//! Index changes: `create_index`,
//! `drop_index` and `replace_index` commit a new `moraine.indexes` property.
//!
//! A change is a metadata-only log entry. Under D9 a writer that loses the race to another
//! commit reloads and applies its change again: creating a name that now exists with an equal
//! spec succeeds, with a different spec fails with [`Error::IndexExists`]; dropping a name that
//! is gone succeeds. A new spec hash means old sidecars stop matching at once, and every file
//! takes the exact route until the builder catches up.

use std::sync::{Arc, Mutex};

use futures::FutureExt;

use super::{Table, TableInner};
use crate::commit::{AttemptError, ConflictKind, commit_with_retries};
use crate::error::{Error, Result};
use crate::index::spec::{INDEXES_PROPERTY, parse_specs, validate_index_name, write_specs};
use crate::index::{BuildOptions, BuildReport, IndexSpec, IndexStatus, Search};
use crate::log::{LogEntry, NewEntry, TableMeta, TxnId};
use crate::snapshot::Snapshot;

/// A change of the table properties: an index change, or a pin (D49).
#[derive(Clone, Debug)]
pub(crate) enum Change {
    Create(IndexSpec),
    Drop(String),
    Replace(IndexSpec),
    /// Sets (`Some`) or removes (`None`) the pin of that name.
    Pin(String, Option<u64>),
    /// Records a purge at the version it follows (D66), fenced by the purge lease.
    Purge(Option<TxnId>),
}

impl Change {
    /// The new properties, or `None` when `config` already is what the change asks for.
    fn configure(
        &self,
        config: &std::collections::BTreeMap<String, String>,
        version: u64,
    ) -> Result<Option<std::collections::BTreeMap<String, String>>> {
        if let Change::Purge(_) = self {
            if super::pins::purged(config)? == Some(version) {
                return Ok(None);
            }
            let mut c = config.clone();
            c.insert(super::pins::PURGED_PROPERTY.into(), version.to_string());
            return Ok(Some(c));
        }
        if let Change::Pin(name, version) = self {
            let mut pins = super::pins::parse_pins(config)?;
            let changed = match version {
                Some(v) => pins.insert(name.clone(), *v) != Some(*v),
                None => pins.remove(name).is_some(),
            };
            if !changed {
                return Ok(None);
            }
            let mut c = config.clone();
            c.insert(
                super::pins::PINS_PROPERTY.into(),
                serde_json::to_string(&pins).map_err(|e| Error::Internal(e.to_string()))?,
            );
            return Ok(Some(c));
        }
        let specs = parse_specs(config)?;
        let Some(new) = self.apply(&specs)? else {
            return Ok(None);
        };
        let mut c = config.clone();
        c.insert(INDEXES_PROPERTY.into(), write_specs(&new));
        Ok(Some(c))
    }

    /// The new list, or `None` when `specs` already is what the change asks for.
    fn apply(&self, specs: &[IndexSpec]) -> Result<Option<Vec<IndexSpec>>> {
        match self {
            Change::Create(s) => match specs.iter().find(|x| x.name == s.name) {
                Some(x) if x == s => Ok(None),
                Some(_) => Err(Error::IndexExists(s.name.clone())),
                None => {
                    let mut v = specs.to_vec();
                    v.push(s.clone());
                    Ok(Some(v))
                }
            },
            Change::Drop(name) => {
                if specs.iter().any(|x| &x.name == name) {
                    Ok(Some(
                        specs.iter().filter(|x| &x.name != name).cloned().collect(),
                    ))
                } else {
                    Ok(None)
                }
            }
            Change::Replace(s) => {
                if specs.iter().any(|x| x == s) {
                    return Ok(None);
                }
                let mut v: Vec<IndexSpec> =
                    specs.iter().filter(|x| x.name != s.name).cloned().collect();
                v.push(s.clone());
                Ok(Some(v))
            }
            Change::Pin(..) | Change::Purge(_) => Ok(None),
        }
    }
}

impl Table {
    /// The index specs of the snapshot in hand. [`Error::InvalidIndexSpec`] when the table's
    /// property is malformed.
    pub fn indexes(&self) -> Result<Vec<IndexSpec>> {
        parse_specs(&self.inner.current().meta.configuration)
    }

    /// Creates an index. The kind fills every default and checks the columns; the resolved
    /// spec is what is stored and returned. Creating an index that exists with an equal spec
    /// changes nothing. Queries may use it at once; files move from the exact route to the
    /// sidecar route as [`Table::build_indexes`] covers them.
    pub async fn create_index(&self, spec: IndexSpec) -> Result<IndexSpec> {
        let resolved = self.resolve_spec(&spec)?;
        self.inner
            .change_indexes(Change::Create(resolved.clone()))
            .await?;
        Ok(resolved)
    }

    /// Drops an index. Dropping one that does not exist changes nothing. Its sidecars are
    /// collected after the grace period.
    pub async fn drop_index(&self, name: &str) -> Result<()> {
        validate_index_name(name)?;
        self.inner.change_indexes(Change::Drop(name.into())).await?;
        Ok(())
    }

    /// Replaces an index of the same name (a drop and a create in one commit), or creates it.
    pub async fn replace_index(&self, spec: IndexSpec) -> Result<IndexSpec> {
        let resolved = self.resolve_spec(&spec)?;
        self.inner
            .change_indexes(Change::Replace(resolved.clone()))
            .await?;
        Ok(resolved)
    }

    /// A nearest-neighbour search of vector index `index`. Every file answers
    /// from its sidecar when one is built and usable, otherwise by the exact route.
    pub fn search_vector(&self, index: &str, vector: Vec<f32>) -> Search {
        Search::vector(self.inner.clone(), index, vector)
    }

    /// A full-text search of index `index`, in tantivy's
    /// query syntax: words, `"phrases"`, `column:word`, `AND`, `OR`, `-word`, `word^2`. Ranked
    /// by BM25 with statistics summed over every file of the snapshot, so scores equal those
    /// of one index over the whole table whatever the files and whichever have sidecars. Hits
    /// carry the score in `_moraine_score` (larger is better).
    ///
    /// ```no_run
    /// # async fn f(t: moraine::Table) -> moraine::Result<()> {
    /// use moraine::expr::{col, lit};
    /// let r = t
    ///     .search_text("body_fts", "object storage")
    ///     .limit(5)
    ///     .filter(col("lang").eq(lit("en")))
    ///     .select(["id", "title"])
    ///     .collect()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn search_text(&self, index: &str, query: &str) -> Search {
        Search::text(self.inner.clone(), index, query.to_string())
    }

    /// A hybrid search: the vector search of `vector_index` and the
    /// full-text search of `text_index`, against one snapshot, each for `k * oversample` hits
    /// (default 4), fused with reciprocal rank fusion (constant 60) unless another
    /// [`crate::index::hybrid::Fusion`] is given. `_moraine_score` holds the fused score.
    ///
    /// ```no_run
    /// # async fn f(t: moraine::Table, v: Vec<f32>) -> moraine::Result<()> {
    /// let r = t
    ///     .search_hybrid("emb", v, "body_fts", "object storage")
    ///     .limit(10)
    ///     .select(["id", "title"])
    ///     .collect()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    #[cfg(feature = "fts")]
    #[cfg_attr(docsrs, doc(cfg(feature = "fts")))]
    pub fn search_hybrid(
        &self,
        vector_index: &str,
        vector: Vec<f32>,
        text_index: &str,
        text: &str,
    ) -> crate::index::hybrid::HybridSearch {
        crate::index::hybrid::HybridSearch::new(
            self.inner.clone(),
            vector_index,
            vector,
            text_index,
            text,
        )
    }

    /// A search of an index whose kind defines its own query bytes ([`Query::Custom`]).
    ///
    /// [`Query::Custom`]: crate::index::Query::Custom
    pub fn search_custom(&self, index: &str, bytes: Vec<u8>) -> Search {
        Search::custom(self.inner.clone(), index, bytes)
    }

    /// Builds the missing sidecars of the table's indexes for the newest version:
    /// largest files first, within the database's build memory,
    /// skipping files that got a sidecar meanwhile. Maintenance runs this; it is safe to run
    /// from any number of clients at once (case V14).
    pub async fn build_indexes(&self, opts: BuildOptions) -> Result<BuildReport> {
        self.inner.build_indexes(opts).await
    }

    /// Coverage of each index by sidecars. Costs no request beyond a sidecar listing older
    /// than the poll interval.
    pub async fn index_status(&self) -> Result<Vec<IndexStatus>> {
        self.inner.index_status().await
    }

    fn resolve_spec(&self, spec: &IndexSpec) -> Result<IndexSpec> {
        validate_index_name(&spec.name)?;
        let kind = self.inner.db.index.kind(&spec.kind)?;
        let resolved = kind.resolve(spec, &self.schema())?;
        if resolved.name != spec.name || resolved.kind != spec.kind {
            return Err(Error::InvalidIndexSpec(format!(
                "kind {} changed the name or kind of index {}",
                spec.kind, spec.name
            )));
        }
        Ok(resolved)
    }
}

impl TableInner {
    /// The prefix of the table's sidecars: `<db>/<table>/idx`.
    pub(crate) fn idx_prefix(&self) -> object_store::path::Path {
        self.db.root.clone().join(self.name.as_str()).join("idx")
    }

    pub(crate) async fn change_indexes(self: &Arc<Self>, change: Change) -> Result<u64> {
        self.db.require_writable().await?;
        let clock = self.db.clock().clone();
        let txn = TxnId {
            app_id: self.db.writer_id.clone(),
            version: self.db.next_txn(),
        };
        let open: Arc<Mutex<Option<Arc<Snapshot>>>> = Arc::new(Mutex::new(None));
        let attempt = {
            let (me, change, txn, open) = (self.clone(), change.clone(), txn.clone(), open.clone());
            move |_| {
                let (me, change, txn, open) =
                    (me.clone(), change.clone(), txn.clone(), open.clone());
                async move { me.index_attempt(&change, &txn, &open).await }.boxed()
            }
        };
        let resolve = {
            let (me, txn) = (self.clone(), txn.clone());
            move || {
                let (me, open, txn) = (me.clone(), open.clone(), txn.clone());
                async move { me.resolve(open, txn).await }.boxed()
            }
        };
        let (r, _) = commit_with_retries(
            &self.db.opts.commit_policy,
            clock.as_ref(),
            &txn.to_string(),
            attempt,
            resolve,
        )
        .await;
        r
    }

    async fn index_attempt(
        &self,
        change: &Change,
        txn: &TxnId,
        open: &Mutex<Option<Arc<Snapshot>>>,
    ) -> Result<u64, AttemptError> {
        let fatal = |e: Error| {
            if super::transient(&e) {
                AttemptError::Throttled
            } else {
                AttemptError::Fatal(e)
            }
        };
        let snap = self.refresh().await.map_err(fatal)?;
        if let Change::Pin(name, Some(v)) = change {
            let already = super::pins::parse_pins(&snap.meta.configuration)
                .map_err(AttemptError::Fatal)?
                .get(name)
                == Some(v);
            let purged = super::pins::purged(&snap.meta.configuration)
                .map_err(AttemptError::Fatal)?
                .is_some_and(|p| *v < p);
            // Too new, or older than a purge, which may have collected its files (D66); a pin
            // already in place stays as it is.
            if !already && (*v > snap.version || purged) {
                return Err(AttemptError::Fatal(Error::VersionNotAvailable(*v)));
            }
        }
        let fence = match change {
            Change::Purge(f) => f.clone(),
            _ => None,
        };
        if let Some(f) = &fence
            && let Some(current) = snap.txns.get(&f.app_id)
            && current > f.version
        {
            return Err(AttemptError::Fatal(Error::Fenced {
                job: f.app_id.clone(),
                generation: f.version,
                current,
            }));
        }
        let Some(configuration) = change
            .configure(&snap.meta.configuration, snap.version)
            .map_err(AttemptError::Fatal)?
        else {
            return Ok(snap.version);
        };
        let meta = TableMeta {
            configuration,
            ..snap.meta.as_ref().clone()
        };
        let body = self
            .layer
            .encode(
                &NewEntry {
                    meta: Some(meta.clone()),
                    protocol: false,
                    adds: Vec::new(),
                    removes: Vec::new(),
                    txn: Some(txn.clone()),
                    caller_txns: fence.iter().cloned().collect(),
                    operation: "SET TBLPROPERTIES",
                    timestamp: self.db.clock().wall_now().timestamp_millis(),
                    data_change: true,
                },
                snap.schema(),
            )
            .map_err(AttemptError::Fatal)?;
        let v = snap.version + 1;
        match self.layer.put_entry(v, body).await {
            Ok(()) => {
                let own = LogEntry {
                    version: v,
                    meta: Some(Arc::new(meta)),
                    txns: std::iter::once(txn.clone()).chain(fence).collect(),
                    ..Default::default()
                };
                let mut s = snap.apply(&[own]).map_err(AttemptError::Fatal)?;
                s.confirmed_at = self.db.clock().now();
                self.install(s);
                Ok(v)
            }
            Err(AttemptError::Ambiguous(m)) => {
                if let Ok(mut g) = open.lock() {
                    *g = Some(snap.clone());
                }
                Err(AttemptError::Ambiguous(m))
            }
            Err(AttemptError::Conflict(_)) => {
                // Another commit took the version: reload and apply the change again (D9).
                Err(AttemptError::Conflict(ConflictKind::MetadataChanged))
            }
            Err(e) => Err(e),
        }
    }
}
